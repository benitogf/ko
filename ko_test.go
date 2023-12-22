package lvlmap

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
