package ko

import (
	"os"
	"runtime"
	"testing"

	"github.com/benitogf/ooo"
)

func TestStorageLlvmap(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := &ooo.Server{}
	app.Silence = true
	app.Storage = &Storage{Path: "test/db"}
	app.Start("localhost:0")
	defer app.Close(os.Interrupt)
	ooo.StorageListTest(app, t)
	ooo.StorageObjectTest(app, t)
}

func TestStreamBroadcastLevel(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := ooo.Server{}
	app.Silence = true
	app.ForcePatch = true
	app.Storage = &Storage{Path: "test/db1" + ooo.Time()}
	app.Start("localhost:0")
	app.Storage.Clear()
	defer app.Close(os.Interrupt)
	ooo.StreamBroadcastTest(t, &app)
}

func TestStreamGlobBroadcastLevel(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := ooo.Server{}
	app.Silence = true
	app.ForcePatch = true
	app.Storage = &Storage{Path: "test/db2" + ooo.Time()}
	app.Start("localhost:0")
	app.Storage.Clear()
	defer app.Close(os.Interrupt)
	ooo.StreamGlobBroadcastTest(t, &app, 5)
}

func TestStreamGlobBroadcastConcurrentMemory(t *testing.T) {
	t.Skip()
	// t.Parallel()
	app := ooo.Server{}
	app.Silence = true
	app.ForcePatch = true
	app.Start("localhost:0")
	defer app.Close(os.Interrupt)
	ooo.StreamGlobBroadcastConcurrentTest(t, &app, 3)
}

func TestStreamBroadcastFilter(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := ooo.Server{}
	app.Silence = true
	app.ForcePatch = true
	app.Storage = &Storage{Path: "test/db3" + ooo.Time()}
	defer app.Close(os.Interrupt)
	ooo.StreamBroadcastFilterTest(t, &app)
}

func TestGetN(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := &ooo.Server{}
	app.Silence = true
	app.Start("localhost:0")
	defer app.Close(os.Interrupt)
	ooo.StorageGetNTest(app, t, 5)
}

func TestGetNRange(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := &ooo.Server{}
	app.Silence = true
	app.Start("localhost:0")
	defer app.Close(os.Interrupt)
	ooo.StorageGetNRangeTest(app, t, 5)
}

func TestKeysRange(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := &ooo.Server{}
	app.Silence = true
	app.Storage = &Storage{Path: "test/db4" + ooo.Time()}
	app.Start("localhost:0")
	defer app.Close(os.Interrupt)
	ooo.StorageKeysRangeTest(app, t, 5)
}

func TestStreamItemGlobBroadcastLevel(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Parallel()
	}
	app := ooo.Server{}
	app.Silence = true
	app.ForcePatch = true
	app.Storage = &Storage{Path: "test/db5" + ooo.Time()}
	app.Start("localhost:0")
	app.Storage.Clear()
	defer app.Close(os.Interrupt)
	ooo.StreamItemGlobBroadcastTest(t, &app)
}

func TestBatchSet(t *testing.T) {
	app := &ooo.Server{}
	app.Silence = true
	app.Storage = &Storage{Path: "test/db6" + ooo.Time()}
	app.Start("localhost:0")
	defer app.Close(os.Interrupt)
	ooo.StorageBatchSetTest(app, t, 10)
}

func TestWatchStorageNoop(t *testing.T) {
	db := &Storage{Path: "test/db7" + ooo.Time()}
	err := db.Start(ooo.StorageOpt{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ooo.WatchStorageNoopTest(db, t)
}
