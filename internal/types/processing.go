package types

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	ProcessingProtocol        = 2
	ProcessingJobDocument     = "document"
	ProcessingJobScan         = "scan"
	ProcessingPlanned         = "planned"
	ProcessingEnqueuePending  = "enqueue_pending"
	ProcessingQueued          = "queued"
	ProcessingRunning         = "running"
	ProcessingWaitingExternal = "waiting_external"
	ProcessingRetryWait       = "retry_wait"
	ProcessingSucceeded       = "succeeded"
	ProcessingBlocked         = "blocked"
	ProcessingFailed          = "failed"
	ProcessingCanceled        = "canceled"
	ProcessingSuperseded      = "superseded"
	ProcessingSkipped         = "skipped"
	TypeProcessingStep        = "processing:step"
	ProcessingPhasePrepare    = "prepare"
	ProcessingPhasePublish    = "publish"
	ProcessingPhaseProjection = "projection"
	ProcessingPhaseRetire     = "retire"
	ProcessingPhaseScan       = "scan"
)

// ProcessingJob owns one immutable source/config generation. Current describes
// the desired version; Published describes the version users can retrieve.
// They deliberately need not identify the same job.
type ProcessingJob struct {
	ID                    string     `json:"id" gorm:"primaryKey;size:64"`
	Kind                  string     `json:"kind" gorm:"size:16;not null"`
	TenantID              uint64     `json:"tenant_id" gorm:"not null;index"`
	KnowledgeBaseID       string     `json:"knowledge_base_id" gorm:"size:64;not null;index"`
	DataSourceID          string     `json:"datasource_id" gorm:"column:datasource_id;size:64;not null;index"`
	ExternalID            string     `json:"external_id" gorm:"size:512;not null"`
	OriginRunID           string     `json:"origin_run_id" gorm:"size:64;index"`
	Generation            int64      `json:"generation" gorm:"not null"`
	ScopeRevision         string     `json:"scope_revision" gorm:"size:64;not null"`
	AuthRevision          string     `json:"auth_revision,omitempty" gorm:"size:64"`
	SourceRevision        string     `json:"source_revision" gorm:"size:256;not null"`
	SourceDigest          string     `json:"source_digest,omitempty" gorm:"size:64"`
	PipelineFingerprint   string     `json:"pipeline_fingerprint" gorm:"size:64;not null"`
	ConfigurationRevision string     `json:"configuration_revision" gorm:"size:64;not null"`
	KnowledgeID           string     `json:"knowledge_id,omitempty" gorm:"size:64;index"`
	IsCurrent             bool       `json:"is_current" gorm:"not null;default:false"`
	IsPublished           bool       `json:"is_published" gorm:"not null;default:false"`
	PublicationEpoch      int64      `json:"publication_epoch" gorm:"not null;default:0"`
	ActiveIndexManifest   string     `json:"active_index_manifest,omitempty"`
	IndexDestination      JSON       `json:"index_destination,omitempty" gorm:"type:jsonb"`
	RollbackPin           bool       `json:"rollback_pin" gorm:"not null;default:false"`
	RetirementState       string     `json:"retirement_state" gorm:"size:32;not null;default:retained"`
	Completeness          string     `json:"completeness" gorm:"size:32;not null;default:unknown"`
	Readiness             string     `json:"readiness" gorm:"size:32;not null;default:pending"`
	Status                string     `json:"status" gorm:"size:32;not null;default:planned;index"`
	Revision              int64      `json:"revision" gorm:"not null;default:0"`
	PlanSealed            bool       `json:"plan_sealed" gorm:"not null;default:false"`
	PlanDigest            string     `json:"plan_digest,omitempty" gorm:"size:64"`
	Metadata              JSON       `json:"metadata,omitempty" gorm:"type:jsonb"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	PublishedAt           *time.Time `json:"published_at,omitempty"`
	FinishedAt            *time.Time `json:"finished_at,omitempty"`
}

func (ProcessingJob) TableName() string { return "processing_jobs" }

// Persisted before the first index write. Cleanup must use the original store
// and vector dimension even if the KB, model or tenant defaults have changed.
type ProcessingIndexDestination struct {
	EnvironmentDigest string                  `json:"environment_digest,omitempty"`
	VectorStoreID     *string                 `json:"vector_store_id,omitempty"`
	Engines           []RetrieverEngineParams `json:"engines"`
	Kinds             []RetrieverType         `json:"kinds"`
	Dimension         int                     `json:"dimension"`
	KnowledgeType     string                  `json:"knowledge_type"`
}

type ProcessingControlRequest struct {
	ExpectedRevision   int64  `json:"expected_revision"`
	OperationRequestID string `json:"operation_request_id"`
	Reason             string `json:"reason"`
	Actor              string `json:"-"`
}

type ProcessingExportResolution struct {
	ProcessingControlRequest
	StepID            string `json:"step_id"`
	FileID            string `json:"file_id"`
	SourceRevision    string `json:"source_revision"`
	RequestDigest     string `json:"request_digest"`
	TaskID            string `json:"task_id"`
	NotStarted        bool   `json:"not_started"`
	EvidenceReference string `json:"evidence_reference"`
}

type processingLeaseContextKey struct{}

func WithProcessingLease(ctx context.Context, lease ProcessingLease) context.Context {
	return context.WithValue(ctx, processingLeaseContextKey{}, lease)
}

func ProcessingLeaseFromContext(ctx context.Context) (ProcessingLease, bool) {
	lease, ok := ctx.Value(processingLeaseContextKey{}).(ProcessingLease)
	return lease, ok
}

// ProcessingStep is the authoritative state of a retryable execution unit.
// A barrier seals its unit plan before checking readiness; an absent/zero
// legacy counter is never evidence that its expected children completed.
type ProcessingStep struct {
	ID                       string     `json:"id" gorm:"primaryKey;size:64"`
	JobID                    string     `json:"job_id" gorm:"size:64;not null;uniqueIndex:uq_processing_step,priority:1;index"`
	ParentStepID             string     `json:"parent_step_id,omitempty" gorm:"size:64;index"`
	Stage                    string     `json:"stage" gorm:"size:64;not null;uniqueIndex:uq_processing_step,priority:2"`
	UnitKey                  string     `json:"unit_key" gorm:"size:512;not null;uniqueIndex:uq_processing_step,priority:3"`
	Kind                     string     `json:"kind" gorm:"size:16;not null;default:work"`
	Phase                    string     `json:"phase" gorm:"size:32;not null"`
	Dependencies             JSON       `json:"dependencies,omitempty" gorm:"type:jsonb"`
	Input                    JSON       `json:"input,omitempty" gorm:"type:jsonb"`
	Status                   string     `json:"status" gorm:"size:32;not null;default:planned;index"`
	Attempt                  int        `json:"step_attempt" gorm:"column:step_attempt;not null;default:1"`
	DispatchSeq              int64      `json:"dispatch_seq" gorm:"not null;default:0"`
	LeaseToken               string     `json:"-" gorm:"size:64"`
	LeaseExpiresAt           *time.Time `json:"lease_expires_at,omitempty" gorm:"index"`
	HeartbeatAt              *time.Time `json:"heartbeat_at,omitempty"`
	ProgressAt               *time.Time `json:"progress_at,omitempty"`
	InputFingerprint         string     `json:"input_fingerprint" gorm:"size:64;not null"`
	ExpectedPublicationEpoch int64      `json:"expected_publication_epoch" gorm:"not null;default:0"`
	CheckpointRef            string     `json:"checkpoint_ref,omitempty"`
	OutputManifestRef        string     `json:"output_manifest_ref,omitempty"`
	OutputDigest             string     `json:"output_digest,omitempty" gorm:"size:64"`
	PlanSealed               bool       `json:"plan_sealed" gorm:"not null;default:false"`
	PlanDigest               string     `json:"plan_digest,omitempty" gorm:"size:64"`
	ExpectedUnits            int        `json:"expected_units" gorm:"not null;default:0"`
	RequiredForReady         bool       `json:"required_for_ready" gorm:"not null;default:false"`
	RequiredForCompletion    bool       `json:"required_for_completion" gorm:"not null;default:false"`
	RetryCount               int        `json:"retry_count" gorm:"not null;default:0"`
	MaxRetries               int        `json:"max_retries" gorm:"not null;default:4"`
	NextRunAt                *time.Time `json:"next_run_at,omitempty" gorm:"index"`
	DeadlineAt               *time.Time `json:"deadline_at,omitempty"`
	ErrorClass               string     `json:"error_class,omitempty" gorm:"size:64"`
	ErrorCode                string     `json:"error_code,omitempty" gorm:"size:64"`
	ErrorMessage             string     `json:"error_message,omitempty"`
	LastErrorEventID         *int64     `json:"last_error_event_id,omitempty"`
	QueueTaskID              string     `json:"queue_task_id,omitempty" gorm:"size:256"`
	Result                   JSON       `json:"result,omitempty" gorm:"type:jsonb"`
	CreatedAt                time.Time  `json:"created_at"`
	UpdatedAt                time.Time  `json:"updated_at"`
	QueuedAt                 *time.Time `json:"queued_at,omitempty"`
	StartedAt                *time.Time `json:"started_at,omitempty"`
	FinishedAt               *time.Time `json:"finished_at,omitempty"`
}

func (ProcessingStep) TableName() string { return "processing_steps" }

// ProcessingEvent preserves transitions and explicit recovery evidence. These
// rows are append-only; projections must not erase an error to clear an alert.
type ProcessingEvent struct {
	ID                 int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	TenantID           uint64    `json:"tenant_id" gorm:"not null;index"`
	JobID              string    `json:"job_id" gorm:"size:64;not null;uniqueIndex:uq_processing_event_revision,priority:1;index"`
	JobRevision        int64     `json:"job_revision" gorm:"not null;uniqueIndex:uq_processing_event_revision,priority:2"`
	RunID              string    `json:"run_id,omitempty" gorm:"size:64;index"`
	StepID             string    `json:"step_id,omitempty" gorm:"size:64;index"`
	Generation         int64     `json:"generation" gorm:"not null"`
	Attempt            int       `json:"step_attempt,omitempty" gorm:"column:step_attempt"`
	DispatchSeq        int64     `json:"dispatch_seq,omitempty"`
	LeaseToken         string    `json:"-" gorm:"size:64"`
	Type               string    `json:"event_type" gorm:"column:event_type;size:64;not null;index"`
	FromState          string    `json:"from_state,omitempty" gorm:"size:32"`
	ToState            string    `json:"to_state,omitempty" gorm:"size:32"`
	ErrorClass         string    `json:"error_class,omitempty" gorm:"size:64"`
	ErrorCode          string    `json:"error_code,omitempty" gorm:"size:64"`
	Message            string    `json:"message,omitempty"`
	Actor              string    `json:"actor,omitempty" gorm:"size:128"`
	Action             string    `json:"action,omitempty" gorm:"size:64"`
	OperationRequestID string    `json:"operation_request_id,omitempty" gorm:"size:128"`
	QueueTaskID        string    `json:"queue_task_id,omitempty" gorm:"size:256"`
	TraceID            string    `json:"trace_id,omitempty" gorm:"size:128"`
	ResolvesEventID    *int64    `json:"resolves_event_id,omitempty" gorm:"index"`
	ResolutionType     string    `json:"resolution_type,omitempty" gorm:"size:64"`
	Detail             JSON      `json:"detail,omitempty" gorm:"type:jsonb"`
	CreatedAt          time.Time `json:"created_at"`
}

func (ProcessingEvent) TableName() string { return "processing_events" }

type SyncRunItem struct {
	RunID          string    `json:"run_id" gorm:"primaryKey;size:64"`
	ItemKey        string    `json:"item_key" gorm:"primaryKey;size:512"`
	TenantID       uint64    `json:"tenant_id" gorm:"not null;index"`
	Kind           string    `json:"kind" gorm:"size:16;not null"`
	ExternalID     string    `json:"external_id" gorm:"size:512;not null"`
	JobID          string    `json:"job_id,omitempty" gorm:"size:64;index"`
	Disposition    string    `json:"disposition" gorm:"size:32;not null"`
	SourceRevision string    `json:"source_revision,omitempty" gorm:"size:256"`
	CreatedAt      time.Time `json:"discovered_at"`
}

func (SyncRunItem) TableName() string { return "sync_run_items" }

// A consumer keeps a confirmed immutable output alive until its own retirement.
// References are never expired by wall-clock TTL while the consumer still exists.
type ProcessingArtifactReference struct {
	ID             string    `json:"id" gorm:"size:36;primaryKey"`
	TenantID       uint64    `json:"tenant_id" gorm:"not null;index"`
	ProducerJobID  string    `json:"producer_job_id" gorm:"size:36;not null;index"`
	ProducerStepID string    `json:"producer_step_id" gorm:"size:36;not null;uniqueIndex:uq_processing_artifact_reference"`
	ConsumerJobID  string    `json:"consumer_job_id" gorm:"size:36;not null;index"`
	ConsumerStepID string    `json:"consumer_step_id" gorm:"size:36;not null;uniqueIndex:uq_processing_artifact_reference"`
	Attempt        int       `json:"attempt" gorm:"not null"`
	Digest         string    `json:"digest" gorm:"size:64;not null"`
	CreatedAt      time.Time `json:"created_at"`
}

func (ProcessingArtifactReference) TableName() string { return "processing_artifact_references" }

// ProcessingRef is the immutable delivery identity. Lease tokens are allocated
// by a successful claim, never supplied by a producer or exposed in API JSON.
type ProcessingRef struct {
	Protocol         int    `json:"protocol"`
	JobID            string `json:"job_id"`
	StepID           string `json:"step_id"`
	Generation       int64  `json:"generation"`
	Attempt          int    `json:"step_attempt"`
	DispatchSeq      int64  `json:"dispatch_seq"`
	InputFingerprint string `json:"input_fingerprint"`
}

type ProcessingTaskPayload struct {
	ProcessingRef
	TenantID uint64 `json:"tenant_id"`
}

// ProcessingQueue preserves the existing hard isolation between worker pools.
func ProcessingQueue(stage string) string {
	switch stage {
	case "scan_page", "scan_document", "discover", "metadata", "fetch", "native_read", "export_start", "export_poll", "download", "normalize":
		return QueueSync
	case "summary", "embedding", "faq_embedding":
		return QueueSummary
	case "asset", "assets", "asset_download", "images", "ocr", "image_ocr", "image_caption", "image_index":
		return QueueMultimodal
	case "graph", "graph_extract", "graph_apply":
		return QueueGraph
	case "question", "questions", "question_index":
		return QueueQuestion
	case "wiki", "wiki_extract", "wiki_dedup", "wiki_cite", "wiki_summary_part", "wiki_summary", "wiki_prepare", "wiki_taxonomy_input", "wiki_taxonomy", "wiki_taxonomy_vectors", "wiki_taxonomy_plan", "wiki_pages", "wiki_page", "wiki_links":
		return QueueWiki
	case "index", "text_index", "summary_index", "faq_prepare", "faq_index", "faq_entry", "faq_write", "publish":
		return QueuePostProcess
	case "retire", "retire_previous", "wiki_retire_page", "cleanup":
		return QueueMaintenance
	default:
		return QueueDefault
	}
}

// ProcessingStepSpec names dependencies as stage/unit pairs within the plan.
// The store resolves them to deterministic step IDs and rejects cycles.
type ProcessingStepSpec struct {
	Stage                 string
	UnitKey               string
	Phase                 string
	Kind                  string
	DependsOn             []string
	InputFingerprint      string
	Input                 JSON
	RequiredForReady      bool
	RequiredForCompletion bool
}

type ProcessingLease struct {
	Ref       ProcessingRef
	Token     string
	ExpiresAt time.Time
	Job       ProcessingJob
	Step      ProcessingStep
}

type ProcessingOutcome struct {
	FAQMutations      []ProcessingFAQMutation
	FAQConflicts      []string
	WikiPage          *ProcessingWikiMutation
	GraphWriteID      string
	DiscoveredItems   []SyncRunItem
	AdmitDocuments    []ProcessingAdmission
	Candidate         *Knowledge
	Chunks            []*Chunk
	IndexedChunkIDs   []string
	Description       *string
	Questions         *ProcessingChunkQuestions
	Status            string
	OutputManifestRef string
	OutputDigest      string
	CheckpointRef     string
	Result            JSON
	Completeness      string
	ErrorClass        string
	ErrorCode         string
	Message           string
	Retryable         bool
	RetryAfter        time.Duration
	NextRunAt         *time.Time
	SealPlan          bool
	ChildSteps        []ProcessingStepSpec
}

// FAQ proposals remain in encrypted artifacts until the complete source batch
// is published. Manifest is explicit because Chunk hides it from public JSON.
type ProcessingFAQMutation struct {
	Chunk        *Chunk   `json:"chunk"`
	Manifest     string   `json:"manifest"`
	NewEntry     bool     `json:"new_entry"`
	EntryStepID  string   `json:"entry_step_id"`
	Attempt      int      `json:"attempt"`
	ResourceRefs []string `json:"resource_refs,omitempty"`
}

// Page content and evidence live only in the encrypted stage artifact. The
// ledger below keeps the identity and exact immutable artifact reference.
type ProcessingWikiMutation struct {
	PlannedPath         []string          `json:"planned_path,omitempty"`
	Maintenance         bool              `json:"maintenance,omitempty"`
	LinkedRevisions     map[string]string `json:"linked_revisions,omitempty"`
	PreserveUntracked   bool              `json:"preserve_untracked,omitempty"`
	RetireOnly          bool              `json:"retire_only,omitempty"`
	Page                *WikiPage         `json:"page"`
	ExpectedRevision    int64             `json:"expected_revision"`
	PublicationEpoch    int64             `json:"publication_epoch"`
	RetireIDs           []string          `json:"retire_ids"`
	ChunkRevisions      map[string]int    `json:"chunk_revisions"`
	ContributionContent string            `json:"contribution_content"`
	ContributionTitle   string            `json:"contribution_title"`
}

type ProcessingWikiWrite struct {
	ID               string `gorm:"type:varchar(36);primaryKey"`
	TenantID         uint64 `gorm:"not null;index"`
	JobID            string `gorm:"type:varchar(36);not null;index"`
	StepID           string `gorm:"type:varchar(36);not null"`
	Attempt          int    `gorm:"column:step_attempt;not null"`
	PublicationEpoch int64  `gorm:"not null"`
	KnowledgeBaseID  string `gorm:"type:varchar(36);not null;index"`
	KnowledgeID      string `gorm:"type:varchar(36);not null"`
	PageID           string `gorm:"type:varchar(36);not null;index"`
	Slug             string `gorm:"type:varchar(255);not null"`
	State            string `gorm:"type:varchar(16);not null"`
	ArtifactRef      string `gorm:"type:text;not null" json:"-"`
	ArtifactDigest   string `gorm:"type:varchar(64);not null" json:"-"`
	InputFingerprint string `gorm:"type:varchar(64);not null" json:"-"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// ProcessingGraphWrite records an immutable external namespace before I/O.
// Confirmation and the active artifact manifest are committed together.
type ProcessingGraphWrite struct {
	ID                string `gorm:"type:varchar(36);primaryKey"`
	TenantID          uint64 `gorm:"not null;index"`
	JobID             string `gorm:"type:varchar(36);not null;index"`
	StepID            string `gorm:"type:varchar(36);not null"`
	Attempt           int    `gorm:"column:step_attempt;not null"`
	PublicationEpoch  int64  `gorm:"not null"`
	KnowledgeBaseID   string `gorm:"type:varchar(36);not null;index"`
	KnowledgeID       string `gorm:"type:varchar(36);not null"`
	ChunkID           string `gorm:"type:varchar(36);not null"`
	ContentRevision   int    `gorm:"not null"`
	DestinationDigest string `gorm:"type:varchar(64);not null" json:"-"`
	OutputDigest      string `gorm:"type:varchar(64);not null"`
	State             string `gorm:"type:varchar(16);not null"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type ProcessingChunkQuestions struct {
	ChunkID         string              `json:"chunk_id"`
	ContentRevision int                 `json:"content_revision"`
	Questions       []GeneratedQuestion `json:"questions"`
}

// A scan commit admits the document, its sealed plan and run membership in
// the same transaction as the scan unit's completion receipt.
type ProcessingAdmission struct {
	Job   ProcessingJob
	Steps []ProcessingStepSpec
}

// Compact enough for every existing vector driver's source_id column. The
// source unit can be a chunk, image or generated question UUID; step/attempt
// identifies the exact confirmed index write independently of a backend ID.
func ProcessingIndexSourceID(stepID string, attempt int, unitID string) (string, error) {
	step, err := uuid.Parse(stepID)
	if err != nil {
		return "", err
	}
	unit, err := uuid.Parse(unitID)
	if err != nil {
		return "", err
	}
	if attempt < 1 || uint64(attempt) > uint64(^uint32(0)) {
		return "", errors.New("invalid index attempt")
	}
	var data [36]byte
	copy(data[:16], step[:])
	binary.BigEndian.PutUint32(data[16:20], uint32(attempt))
	copy(data[20:], unit[:])
	return "p2_" + base64.RawURLEncoding.EncodeToString(data[:]), nil
}

func ParseProcessingIndexSourceID(source string) (string, int, error) {
	if len(source) != 51 || source[:3] != "p2_" {
		return "", 0, errors.New("invalid processing index source")
	}
	data, err := base64.RawURLEncoding.DecodeString(source[3:])
	if err != nil || len(data) != 36 {
		return "", 0, errors.New("invalid processing index source")
	}
	step, err := uuid.FromBytes(data[:16])
	attempt := binary.BigEndian.Uint32(data[16:20])
	if err != nil || attempt == 0 {
		return "", 0, errors.New("invalid processing index identity")
	}
	return step.String(), int(attempt), nil
}

type ProcessingArtifactVersion struct {
	Attempt int    `json:"attempt"`
	Digest  string `json:"digest"`
}

func ProcessingKnowledgeID(jobID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("processing-knowledge/"+jobID)).String()
}

// StorageUsed includes these charges from reservation until confirmed removal.
// Committing a file transfers its reservation; it does not charge twice.
type ProcessingStorageReservation struct {
	StorageBackendID string `json:"-" gorm:"type:varchar(36);not null;default:''"`
	ID               string `gorm:"type:varchar(36);primaryKey"`
	TenantID         uint64 `gorm:"not null;index"`
	JobID            string `gorm:"type:varchar(64);not null;index"`
	StepID           string `gorm:"type:varchar(64);not null"`
	Attempt          int    `gorm:"not null"`
	Bytes            int64  `gorm:"not null"`
	Temporary        bool   `gorm:"not null;default:false"`
	Kind             string `gorm:"type:varchar(16);not null;default:file"`
	State            string `gorm:"type:varchar(16);not null"`
	ResourceID       string `gorm:"type:varchar(36);not null;default:'';index"`
	PhysicalPath     string `json:"-" gorm:"not null;default:''"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type processingStorageContextKey struct{}

func WithProcessingStorageReservation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, processingStorageContextKey{}, id)
}

func ProcessingStorageReservationFromContext(ctx context.Context) string {
	id, _ := ctx.Value(processingStorageContextKey{}).(string)
	return id
}
