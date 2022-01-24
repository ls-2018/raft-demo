package raft

import (
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)

func TestFileSnapshotStoreImpl(t *testing.T) {
	var impl interface{} = &FileSnapshotStore{}
	if _, ok := impl.(SnapshotStore); !ok {
		t.Fatalf("FileSnapshotStore not a SnapshotStore")
	}
}

func TestFileSnapshotSinkImpl(t *testing.T) {
	var impl interface{} = &FileSnapshotSink{}
	if _, ok := impl.(SnapshotSink); !ok {
		t.Fatalf("FileSnapshotSink not a SnapshotSink")
	}
}

func TestFileSS_BadPerm(t *testing.T) {
	var err error
	if runtime.GOOS == "windows" {
		t.Skip("skipping file permission test on windows")
	}

	// Create a temp dir
	var dir1 string
	dir1, err = ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.RemoveAll(dir1)

	// Create a sub dir and remove all permissions
	var dir2 string
	dir2, err = ioutil.TempDir(dir1, "badperm")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err = os.Chmod(dir2, 000); err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.Chmod(dir2, 777) // Set perms back for delete

	// Should fail
	if _, err = NewFileSnapshotStore(dir2, 3, nil); err == nil {
		t.Fatalf("should fail to use dir with bad perms")
	}
}

func TestFileSS_MissingParentDir(t *testing.T) {
	parent, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}
	defer os.RemoveAll(parent)

	dir, err := ioutil.TempDir(parent, "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	os.RemoveAll(parent)
	_, err = NewFileSnapshotStore(dir, 3, nil)
	if err != nil {
		t.Fatalf("should not fail when using non existing parent")
	}
}
