package tools

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"os"
)

// excelWithExplicitNumberFormats fills only omitted OOXML numFmtId defaults.
// Other ZIP entries are copied byte-for-byte without interpreting cell data.
func excelWithExplicitNumberFormats(filename string) (result string, err error) {
	z, err := zip.OpenReader(filename)
	if err != nil {
		return "", err
	}
	defer z.Close()
	var styles *zip.File
	for _, f := range z.File {
		if f.Name == "xl/styles.xml" {
			if styles != nil {
				return "", errors.New("duplicate Excel styles")
			}
			styles = f
		}
	}
	if styles == nil {
		return "", errors.New("Excel styles missing")
	}
	r, err := styles.Open()
	if err != nil {
		return "", err
	}
	defer r.Close()
	const maxStyles = 16 << 20
	raw, err := io.ReadAll(io.LimitReader(r, maxStyles+1))
	if err != nil {
		return "", err
	}
	if len(raw) > maxStyles {
		return "", errors.New("Excel styles too large")
	}
	var out bytes.Buffer
	d, e := xml.NewDecoder(bytes.NewReader(raw)), xml.NewEncoder(&out)
	changed := false
	for {
		token, readErr := d.Token()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
		if start, ok := token.(xml.StartElement); ok {
			attrs := start.Attr[:0]
			for _, attr := range start.Attr {
				// Encoder emits the default namespace from Name.Space itself.
				if attr.Name.Space != "" || attr.Name.Local != "xmlns" {
					attrs = append(attrs, attr)
				}
			}
			start.Attr = attrs
			if start.Name.Local == "xf" && start.Name.Space == "http://schemas.openxmlformats.org/spreadsheetml/2006/main" {
				found := false
				for _, attr := range start.Attr {
					if attr.Name.Local == "numFmtId" {
						found = true
					}
				}
				if !found {
					start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "numFmtId"}, Value: "0"})
					changed = true
				}
			}
			token = start
		}
		if err = e.EncodeToken(token); err != nil {
			return "", err
		}
	}
	if err = e.Flush(); err != nil {
		return "", err
	}
	if !changed {
		return "", errors.New("Excel styles need no default repair")
	}
	tmp, err := os.CreateTemp("", "weknora-excel-styles-*.xlsx")
	if err != nil {
		return "", err
	}
	defer func() {
		tmp.Close()
		if err != nil {
			os.Remove(tmp.Name())
		}
	}()
	w := zip.NewWriter(tmp)
	for _, f := range z.File {
		if f == styles {
			header := f.FileHeader
			var dst io.Writer
			dst, err = w.CreateHeader(&header)
			if err == nil {
				_, err = dst.Write(out.Bytes())
			}
		} else {
			err = w.Copy(f)
		}
		if err != nil {
			w.Close()
			return "", err
		}
	}
	if err = w.Close(); err != nil {
		return "", err
	}
	if err = tmp.Close(); err != nil {
		return "", err
	}
	return tmp.Name(), nil
}
