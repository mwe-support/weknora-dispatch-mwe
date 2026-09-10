package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/Tencent/WeKnora/internal/utils"
)

const (
	processingArtifactLimit = 100 << 20
	processingArtifactBlock = 64 << 10
	processingArtifactMagic = "WKPA3\n"
)

// ProcessingArtifacts stores private, immutable stage outputs with the existing
// file backend and AES-GCM key. Public resources contain ciphertext only.
type ProcessingArtifacts struct {
	files   interfaces.FileService
	catalog interfaces.ResourceCatalog
}

func NewProcessingArtifacts(files interfaces.FileService, catalog interfaces.ResourceCatalog) *ProcessingArtifacts {
	return &ProcessingArtifacts{files: files, catalog: catalog}
}

type processingArtifactHeader struct {
	Protocol   int    `json:"protocol"`
	Tenant     uint64 `json:"tenant"`
	Job        string `json:"job"`
	Generation int64  `json:"generation"`
	Step       string `json:"step"`
	Input      string `json:"input"`
	Kind       string `json:"kind"`
	Bytes      int    `json:"bytes"`
}

func processingArtifactIdentity(ctx context.Context, job types.ProcessingJob, step types.ProcessingStep, kind string) (processingArtifactHeader, error) {
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant != job.TenantID || job.ID == "" || job.Generation < 1 || step.ID == "" || step.InputFingerprint == "" || kind == "" || len(kind) > 64 {
		return processingArtifactHeader{}, errors.New("invalid processing artifact identity")
	}
	return processingArtifactHeader{Protocol: types.ProcessingProtocol, Tenant: tenant, Job: job.ID,
		Generation: job.Generation, Step: step.ID, Input: step.InputFingerprint, Kind: kind}, nil
}

func (s *ProcessingArtifacts) Save(ctx context.Context, job types.ProcessingJob, step types.ProcessingStep, kind string, data []byte) (string, string, error) {
	ctx = types.WithProcessingLease(ctx, types.ProcessingLease{Job: job, Step: step, Token: step.LeaseToken,
		Ref: types.ProcessingRef{Protocol: types.ProcessingProtocol, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}})
	header, err := processingArtifactIdentity(ctx, job, step, kind)
	if err != nil {
		return "", "", err
	}
	if len(data) > processingArtifactLimit {
		return "", "", errors.New("processing artifact exceeds 100 MiB")
	}
	key := utils.GetAESKey()
	if key == nil {
		return "", "", utils.ErrEncryptedDataMissingKey
	}
	header.Bytes = len(data)
	encoded, err := json.Marshal(header)
	if err != nil {
		return "", "", err
	}
	if len(encoded) > 8192 {
		return "", "", errors.New("processing artifact header exceeds limit")
	}
	sum := sha256.New()
	_, _ = sum.Write(encoded)
	_, _ = sum.Write([]byte{'\n'})
	_, _ = sum.Write(data)
	digest := fmt.Sprintf("%x", sum.Sum(nil))
	encryptedSize := int64(len(processingArtifactMagic) + 4 + len(encoded) + len(data) + 28*max(1, (len(data)+processingArtifactBlock-1)/processingArtifactBlock))
	path, err := s.saveStream(ctx, job.TenantID, "processing-"+digest+".enc", encryptedSize, func(w io.Writer) error {
		return writeProcessingArtifact(ctx, w, key, encoded, data)
	})
	if err != nil {
		return "", "", err
	}
	// Verify the stored bytes before the caller may advance its checkpoint.
	if _, err = s.read(ctx, job, step, kind, path, digest, false); err != nil {
		if s.catalog == nil {
			_ = s.files.DeleteFile(ctx, path)
		}
		return "", "", err
	}
	if s.catalog != nil {
		if err := s.catalog.Bind(ctx, path, "processing_job", job.ID, "stage_artifact"); err != nil {
			// Creation ownership preserves this orphan for fenced cleanup.
			return "", "", err
		}
	}
	return path, digest, nil
}

func (s *ProcessingArtifacts) Read(ctx context.Context, job types.ProcessingJob, step types.ProcessingStep, kind, path, digest string) ([]byte, error) {
	return s.read(ctx, job, step, kind, path, digest, true)
}

func (s *ProcessingArtifacts) read(ctx context.Context, job types.ProcessingJob, step types.ProcessingStep, kind, path, digest string, collect bool) ([]byte, error) {
	expected, err := processingArtifactIdentity(ctx, job, step, kind)
	if err != nil {
		return nil, err
	}
	if path == "" || len(digest) != 64 {
		return nil, errors.New("missing verified processing artifact reference")
	}
	if utils.GetAESKey() == nil {
		return nil, utils.ErrEncryptedDataMissingKey
	}
	reader, err := s.files.GetFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	buffered := bufio.NewReader(reader)
	magic, _ := buffered.Peek(len(processingArtifactMagic))
	if string(magic) == processingArtifactMagic {
		_, _ = buffered.Discard(len(processingArtifactMagic))
		return readProcessingArtifact(ctx, buffered, expected, digest, collect)
	}
	// Read-only compatibility for previously confirmed whole-message envelopes.
	// AES-GCM encoding adds base64 overhead plus a bounded header and nonce.
	const encodedLimit = (processingArtifactLimit+8192)*4/3 + 128
	encrypted, err := io.ReadAll(io.LimitReader(buffered, encodedLimit+1))
	if err != nil {
		return nil, err
	}
	if len(encrypted) > encodedLimit {
		return nil, errors.New("encrypted processing artifact exceeds limit")
	}
	if !strings.HasPrefix(string(encrypted), utils.EncPrefix) {
		return nil, errors.New("unencrypted processing artifact rejected")
	}
	plain, err := utils.DecryptStoredSecret(string(encrypted))
	if err != nil {
		return nil, errors.New("processing artifact authentication failed")
	}
	if fmt.Sprintf("%x", sha256.Sum256([]byte(plain))) != digest {
		return nil, errors.New("processing artifact checksum mismatch")
	}
	headerBytes, data, ok := bytes.Cut([]byte(plain), []byte{'\n'})
	if !ok || len(headerBytes) > 8192 || len(data) > processingArtifactLimit {
		return nil, errors.New("invalid processing artifact envelope")
	}
	var header processingArtifactHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, errors.New("invalid processing artifact header")
	}
	expected.Bytes = len(data)
	if kind == "*" && header.Kind != "" && len(header.Kind) <= 64 {
		expected.Kind = header.Kind // Verification of an already confirmed manifest.
	}
	if header != expected {
		return nil, errors.New("processing artifact belongs to a different input")
	}
	return data, nil
}

// multipart.ReadForm(0) spills the encrypted stream to one private temp file,
// giving existing file drivers a seekable upload without an in-memory copy.
func (s *ProcessingArtifacts) saveStream(ctx context.Context, tenant uint64, name string, size int64, write func(io.Writer) error) (path string, resultErr error) {
	reservation := ""
	temporaryRemoved := false
	if s.catalog != nil {
		var err error
		reservation, err = s.catalog.ReserveStorage(ctx, tenant, size, true, "")
		if err != nil {
			return "", err
		}
		defer func() {
			if !temporaryRemoved {
				return
			}
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, s.catalog.ReleaseStorage(cleanup, tenant, reservation))
		}()
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	formWriter := multipart.NewWriter(writer)
	boundary := formWriter.Boundary()
	finished := make(chan error, 1)
	go func() {
		part, err := formWriter.CreateFormFile("file", name)
		if err == nil {
			err = write(part)
		}
		if err == nil {
			err = formWriter.Close()
		}
		_ = writer.CloseWithError(err)
		finished <- err
	}()
	form, err := multipart.NewReader(reader, boundary).ReadForm(0)
	if err != nil {
		_ = reader.CloseWithError(err)
	}
	writeErr := <-finished
	if form != nil {
		defer func() {
			err := form.RemoveAll()
			temporaryRemoved = err == nil
			resultErr = errors.Join(resultErr, err)
		}()
	}
	if err != nil || writeErr != nil {
		return "", errors.Join(err, writeErr)
	}
	if len(form.File["file"]) != 1 {
		return "", errors.New("processing artifact upload is missing")
	}
	if reservation != "" {
		file, err := form.File["file"][0].Open()
		if err != nil {
			return "", err
		}
		if temporary, ok := file.(*os.File); ok {
			err = s.catalog.SetStoragePath(ctx, tenant, reservation, temporary.Name())
		} else {
			err = errors.New("PROCESSING_TEMPORARY_FILE_UNTRACKED")
		}
		_ = file.Close()
		if err != nil {
			return "", err
		}
	}
	return s.files.SaveFile(ctx, form.File["file"][0], tenant, "")
}

func processingArtifactCipher(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func writeProcessingArtifact(ctx context.Context, w io.Writer, key, header, data []byte) error {
	aead, err := processingArtifactCipher(key)
	if err != nil {
		return err
	}
	prefix := make([]byte, len(processingArtifactMagic)+4)
	copy(prefix, processingArtifactMagic)
	binary.BigEndian.PutUint32(prefix[len(processingArtifactMagic):], uint32(len(header)))
	write := func(data []byte) error {
		n, err := w.Write(data)
		if err == nil && n != len(data) {
			return io.ErrShortWrite
		}
		return err
	}
	if err := write(prefix); err != nil {
		return err
	}
	if err := write(header); err != nil {
		return err
	}
	aad := append(bytes.Clone(header), make([]byte, 4)...)
	nonce := make([]byte, aead.NonceSize())
	buffer := make([]byte, 0, processingArtifactBlock+aead.Overhead())
	for index, start := 0, 0; index == 0 || start < len(data); index, start = index+1, start+processingArtifactBlock {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(aad[len(header):], uint32(index))
		sealed := aead.Seal(buffer[:0], nonce, data[start:min(start+processingArtifactBlock, len(data))], aad)
		if err := write(nonce); err != nil {
			return err
		}
		if err := write(sealed); err != nil {
			return err
		}
	}
	return nil
}

func readProcessingArtifact(ctx context.Context, reader io.Reader, expected processingArtifactHeader, digest string, collect bool) ([]byte, error) {
	var size [4]byte
	if _, err := io.ReadFull(reader, size[:]); err != nil {
		return nil, err
	}
	headerSize := binary.BigEndian.Uint32(size[:])
	if headerSize == 0 || headerSize > 8192 {
		return nil, errors.New("invalid processing artifact header size")
	}
	headerBytes := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, headerBytes); err != nil {
		return nil, err
	}
	var header processingArtifactHeader
	if json.Unmarshal(headerBytes, &header) != nil || header.Bytes < 0 || header.Bytes > processingArtifactLimit {
		return nil, errors.New("invalid processing artifact header")
	}
	expected.Bytes = header.Bytes
	if expected.Kind == "*" && header.Kind != "" && len(header.Kind) <= 64 {
		expected.Kind = header.Kind
	}
	if header != expected {
		return nil, errors.New("processing artifact belongs to a different input")
	}
	aead, err := processingArtifactCipher(utils.GetAESKey())
	if err != nil {
		return nil, err
	}
	aad := append(bytes.Clone(headerBytes), make([]byte, 4)...)
	nonce := make([]byte, aead.NonceSize())
	buffer := make([]byte, processingArtifactBlock+aead.Overhead())
	var result []byte
	if collect {
		result = make([]byte, header.Bytes)
	}
	sum := sha256.New()
	_, _ = sum.Write(headerBytes)
	_, _ = sum.Write([]byte{'\n'})
	for index, start := 0, 0; index == 0 || start < header.Bytes; index, start = index+1, start+processingArtifactBlock {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		length := min(processingArtifactBlock, header.Bytes-start)
		if _, err := io.ReadFull(reader, nonce); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(reader, buffer[:length+aead.Overhead()]); err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(aad[len(headerBytes):], uint32(index))
		plain, err := aead.Open(buffer[:0], nonce, buffer[:length+aead.Overhead()], aad)
		if err != nil {
			return nil, errors.New("processing artifact authentication failed")
		}
		_, _ = sum.Write(plain)
		if collect {
			copy(result[start:], plain)
		}
	}
	var trailing [1]byte
	if n, err := io.ReadFull(reader, trailing[:]); n != 0 || err != io.EOF {
		return nil, errors.New("unexpected processing artifact trailing data")
	}
	if fmt.Sprintf("%x", sum.Sum(nil)) != digest {
		return nil, errors.New("processing artifact checksum mismatch")
	}
	return result, nil
}
