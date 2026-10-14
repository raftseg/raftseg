// Copyright 2026 The raftseg authors.

package seglog

import "github.com/raftseg/raftseg/vfs"

// FS is the filesystem a Log uses; OSFS, the default, is the operating
// system's. Tests substitute filesystems that simulate crashes and errors.
type FS = vfs.FS

// File is an open file of an FS.
type File = vfs.File

// OSFS is the operating system's filesystem.
var OSFS FS = vfs.OS
