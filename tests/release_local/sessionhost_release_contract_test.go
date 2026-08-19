package release_local

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestReleaseMetadataCanBePinnedByTheReleasePipeline(t *testing.T) {
	root := repositoryRoot(t)
	cmd := exec.Command("make", "-s", "print-build-metadata",
		"VERSION=v1.5.0-sessionhost.1",
		"COMMIT=0123456789abcdef0123456789abcdef01234567",
		"BUILD_TIME=2026-08-18T12:00:00Z")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("print-build-metadata failed: %v\n%s", err, output)
	}
	want := strings.Join([]string{
		"version=v1.5.0-sessionhost.1",
		"commit=0123456789abcdef0123456789abcdef01234567",
		"build_time=2026-08-18T12:00:00Z",
	}, "\n")
	if strings.TrimSpace(string(output)) != want {
		t.Fatalf("metadata = %q, want %q", strings.TrimSpace(string(output)), want)
	}
}

func TestSessionHostReleaseWorkflowPublishesPinnedArchives(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "release-sessionhost.yml"))
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	workflow := string(data)
	for _, required := range []string{
		"darwin-amd64",
		"darwin-arm64",
		"linux-amd64",
		"linux-arm64",
		"AGENTS=sessionhost",
		"checksums.txt",
		"gh release create",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("release workflow does not contain %q", required)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
