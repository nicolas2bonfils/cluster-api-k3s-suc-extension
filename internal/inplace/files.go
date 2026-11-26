package inplace

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

const (
	// ConfigDir is where k3s reads its configuration drop-ins at each start:
	// the only files the extension writes.
	ConfigDir = "/etc/rancher/k3s/config.yaml.d"
)

// DefaultImmutableKeys are k3s configuration keys a running server cannot
// take a new value for: a restart with another node-ip leaves its etcd member
// on the old peer URL (a cluster reset is needed), and the cluster and
// service CIDRs, the datastore mode and the data directory are fixed at
// creation. A drop-in change touching one of them (also as key+) is refused.
var DefaultImmutableKeys = []string{"node-ip", "cluster-cidr", "service-cidr", "cluster-init", "data-dir"}

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

// checkFilesChange returns why the change from current to desired files
// cannot be applied in place, or "" when it can (or when nothing changed).
// Accepted: files added or modified directly under ConfigDir, as plain
// content, mode 0644, owned by root, whose YAML changes none of
// immutableKeys. Refused: any other change, including a removal
// (UpdateMachine only gets the desired state, so it could not tell which
// file to delete).
func checkFilesChange(current, desired []k3sFile, immutableKeys []string) string {
	currentByPath := filesByPath(current)
	desiredByPath := filesByPath(desired)
	for p := range currentByPath {
		if _, kept := desiredByPath[p]; !kept {
			return fmt.Sprintf("file %s removed", p)
		}
	}
	for p, file := range desiredByPath {
		if old, found := currentByPath[p]; found && reflect.DeepEqual(old, file) {
			continue
		}
		if reason := checkManagedFile(file); reason != "" {
			return reason
		}
		if reason := immutableKeyChange(currentByPath[p].Content, file.Content, immutableKeys); reason != "" {
			return fmt.Sprintf("file %s: %s", file.Path, reason)
		}
	}
	return ""
}

// immutableKeyChange returns which of keys (or key+) old and new k3s
// configuration YAML set differently, or "" when none.
func immutableKeyChange(oldContent, newContent string, keys []string) string {
	oldValues, err := configValues(oldContent)
	if err != nil {
		return err.Error()
	}
	newValues, err := configValues(newContent)
	if err != nil {
		return err.Error()
	}
	for _, key := range keys {
		for _, k := range []string{key, key + "+"} {
			if !reflect.DeepEqual(oldValues[k], newValues[k]) {
				return fmt.Sprintf("%s cannot change on an existing server", k)
			}
		}
	}
	return ""
}

func configValues(content string) (map[string]any, error) {
	values := map[string]any{}
	if strings.TrimSpace(content) == "" {
		return values, nil
	}
	if err := yaml.Unmarshal([]byte(content), &values); err != nil {
		return nil, fmt.Errorf("not a k3s configuration (YAML map): %w", err)
	}
	return values, nil
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

func filesByPath(files []k3sFile) map[string]k3sFile {
	byPath := make(map[string]k3sFile, len(files))
	for _, f := range files {
		byPath[path.Clean(f.Path)] = f
	}
	return byPath
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
