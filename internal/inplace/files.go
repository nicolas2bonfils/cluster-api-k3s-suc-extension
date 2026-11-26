package inplace

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// ConfigDir is where k3s reads its configuration drop-ins at each start:
	// the only files the extension writes.
	ConfigDir = "/etc/rancher/k3s/config.yaml.d"
)

// k3sFile is a KThreesConfig spec.files entry.
type k3sFile struct {
	Path        string          `json:"path"`
	Owner       string          `json:"owner,omitempty"`
	Permissions string          `json:"permissions,omitempty"`
	Encoding    string          `json:"encoding,omitempty"`
	Content     string          `json:"content,omitempty"`
	ContentFrom json.RawMessage `json:"contentFrom,omitempty"`
}

// bootstrapFiles returns spec.files of a KThreesConfig passed in a hook
// request, and the raw list for patches.
func bootstrapFiles(obj runtime.RawExtension) ([]k3sFile, []any, error) {
	raw := obj.Raw
	if len(raw) == 0 && obj.Object != nil {
		var err error
		if raw, err = json.Marshal(obj.Object); err != nil {
			return nil, nil, fmt.Errorf("marshal bootstrap config: %w", err)
		}
	}
	if len(raw) == 0 {
		return nil, nil, nil
	}
	var config struct {
		Spec struct {
			Files []k3sFile `json:"files"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, nil, fmt.Errorf("decode bootstrap config: %w", err)
	}
	var generic struct {
		Spec struct {
			Files []any `json:"files"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, nil, fmt.Errorf("decode bootstrap config: %w", err)
	}
	return config.Spec.Files, generic.Spec.Files, nil
}

func checkManagedFile(file k3sFile) string {
	switch {
	case path.Dir(file.Path) != ConfigDir:
		return fmt.Sprintf("file %s is not directly under %s", file.Path, ConfigDir)
	case len(file.ContentFrom) > 0 && string(file.ContentFrom) != "null":
		return fmt.Sprintf("file %s: contentFrom is not supported", file.Path)
	case file.Encoding != "":
		return fmt.Sprintf("file %s: encoding %q is not supported", file.Path, file.Encoding)
	case file.Owner != "" && file.Owner != "root:root" && file.Owner != "root":
		return fmt.Sprintf("file %s: owner %q is not supported", file.Path, file.Owner)
	case file.Permissions != "" && strings.TrimLeft(file.Permissions, "0") != "644":
		return fmt.Sprintf("file %s: permissions %q are not supported", file.Path, file.Permissions)
	}
	return ""
}

// configFiles returns the desired ConfigDir drop-ins by file name: the
// files the extension writes on the host (unchanged ones are rewritten as
// they are).
func configFiles(files []k3sFile) map[string]string {
	out := map[string]string{}
	for _, f := range files {
		if path.Dir(path.Clean(f.Path)) != ConfigDir {
			continue
		}
		if checkManagedFile(f) != "" {
			continue
		}
		out[path.Base(f.Path)] = f.Content
	}
	return out
}
