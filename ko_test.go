package ko

import (
	"os"
	"runtime"
	"testing"

	"github.com/benitogf/go-json"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

// TestStorageExtendedKeyChars asserts that ko's leveldb-backed storage works
// end-to-end with keys containing hyphens, dots, and underscores. ko ships
// `key.Match` for glob lookups, so this also verifies glob iteration over
// such keys.
func TestStorageExtendedKeyChars(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := &ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db_extchars" + ooo.Time())
	server.Start("localhost:0")
	server.Storage.Clear()
	defer server.Close(os.Interrupt)

	keys := []string{
		"users/john-doe",
		"logs/2026-05-08",
		"data/report.json",
		"users/jane_doe",
		"550e8400-e29b-41d4-a716-446655440000",
	}

	for _, k := range keys {
		_, err := server.Storage.Set(k, json.RawMessage(`{"key":"`+k+`"}`))
		require.NoError(t, err, "Set %q", k)

		obj, err := server.Storage.Get(k)
		require.NoError(t, err, "Get %q", k)
		require.Contains(t, string(obj.Data), k)
	}

	// Verify glob iteration finds keys with the new characters.
	users, err := server.Storage.GetList("users/*")
	require.NoError(t, err)
	require.Len(t, users, 2)

	logs, err := server.Storage.GetList("logs/*")
	require.NoError(t, err)
	require.Len(t, logs, 1)
}

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

func TestStreamForcePatch(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db_forcepatch" + ooo.Time())
	defer server.Close(os.Interrupt)
	ooo.StreamBroadcastForcePatchTest(t, &server)
}

func TestStreamNoPatch(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db_nopatch" + ooo.Time())
	defer server.Close(os.Interrupt)
	ooo.StreamBroadcastNoPatchTest(t, &server)
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

func TestStreamPatch(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := &ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db_patch" + ooo.Time())
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StreamBroadcastPatchTest(t, server)
}

func TestStreamLimitFilter(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	server := &ooo.Server{}
	server.Silence = true
	server.Storage = newLayeredStorage("test/db_limitfilter" + ooo.Time())
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)
	ooo.StreamLimitFilterTest(t, server)
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

func TestStorageBeforeRead(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	db := newLayeredStorage("test/db_beforeread" + ooo.Time())
	err := db.Start(storage.Options{})
	require.NoError(t, err)
	defer db.Close()
	ooo.StorageBeforeReadTest(db, t)
}

func TestStorageAfterWrite(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	monotonic.Init()
	db := newLayeredStorage("test/db_afterwrite" + ooo.Time())
	err := db.Start(storage.Options{})
	require.NoError(t, err)
	defer db.Close()
	ooo.StorageAfterWriteTest(db, t)
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
