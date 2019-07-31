package replication

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

func TestStartBackupEndInGivenTime(t *testing.T) {
	var syn = &testSyncerSuite{}
	setupTest(t, mysql.MySQLFlavor, syn)
	defer tearDownTest(syn)

	testExecute(t, "RESET MASTER", syn)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()

		testSync(t, nil, syn)

		testExecute(t, "FLUSH LOGS", syn)

		testSync(t, nil, syn)
	}()

	os.RemoveAll("./var")
	timeout := 2 * time.Second

	done := make(chan bool)

	go func() {
		err := syn.b.StartBackup("./var", mysql.Position{Name: "", Pos: uint32(0)}, timeout)
		assert.Nil(t, err)
		done <- true
	}()
	failTimeout := 5 * timeout
	ctx, _ := context.WithTimeout(context.Background(), failTimeout)
	select {
	case <-done:
		return
	case <-ctx.Done():
		assert.Nil(t, errors.New("time out error"))
	}
}
