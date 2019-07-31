package exec2

import (
	"strings"
	"testing"
	"time"
)

func TestExec(t *testing.T) {
	err := ExecTimeout(1*time.Second, "sleep", "10")
	if err != nil && !strings.Contains(err.Error(), "signal: killed") {
		t.Fatal(err)
	}
}
