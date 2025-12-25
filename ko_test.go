package ko

import (
	"os"
	"runtime"
	"testing"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

// newLayeredStorage creates a storage.Layered with memory + embedded layers
func newLayeredStorage(path string) *storage.Layered {
	return storage.New(storage.LayeredConfig{
		Memory:   storage.NewMemoryLayer(),
		Embedded: NewEmbeddedStorage(path),
	})
}

func TestStorage(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	server := &ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db")
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StorageListTest(server, t)
	ooo.StorageObjectTest(server, t)
}

func TestStreamBroadcast(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := ooo.Server{}
	server.Silence = true
	server.ForcePatch = true
	server.Storage = newLayeredStorage("test/db1" + ooo.Time())
	server.Start("localhost:0")
	server.Storage.Clear()
	defer server.Close(os.Interrupt)
	ooo.StreamBroadcastTest(t, &server)
}

func TestStreamGlobBroadcast(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	app := ooo.Server{}
	app.Silence = true
	app.ForcePatch = true
	app.Storage = newLayeredStorage("test/db2" + ooo.Time())
	app.Start("localhost:0")
	app.Storage.Clear()
	defer app.Close(os.Interrupt)
	ooo.StreamGlobBroadcastTest(t, &app, 5)
}

func TestStreamGlobBroadcastConcurrent(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := ooo.Server{}
	server.Silence = true
	server.ForcePatch = true
	server.Storage = newLayeredStorage("test/db8" + ooo.Time())
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StreamGlobBroadcastConcurrentTest(t, &server, 3)
}

func TestStreamBroadcastFilter(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := ooo.Server{}
	server.Silence = true
	server.ForcePatch = true
	server.Storage = newLayeredStorage("test/db3" + ooo.Time())
	defer server.Close(os.Interrupt)
	ooo.StreamBroadcastFilterTest(t, &server)
}

func TestGetN(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	server := &ooo.Server{}
	server.Silence = true
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StorageGetNTest(server, t, 5)
}

func TestGetNRange(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	server := &ooo.Server{}
	server.Silence = true
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StorageGetNRangeTest(server, t, 5)
}

func TestKeysRange(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := &ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db4" + ooo.Time())
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StorageKeysRangeTest(server, t, 5)
}

func TestStreamItemGlobBroadcast(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := ooo.Server{}
	server.Silence = true
	server.ForcePatch = true
	server.Storage = newLayeredStorage("test/db5" + ooo.Time())
	server.Start("localhost:0")
	server.Storage.Clear()
	defer server.Close(os.Interrupt)
	ooo.StreamItemGlobBroadcastTest(t, &server)
}

func TestBatchSet(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := &ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db6" + ooo.Time())
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StorageBatchSetTest(server, t, 10)
}

func TestWatchStorageNoop(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	db := newLayeredStorage("test/db7" + ooo.Time())
	err := db.Start(storage.Options{})
	require.NoError(t, err)
	defer db.Close()
	ooo.WatchStorageNoopTest(db, t)
}

func TestClientCompatibility(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	server := &ooo.Server{}
	server.Silence = true
	server.ForcePatch = true
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.ClientCompatibilityTest(t, server)
}
