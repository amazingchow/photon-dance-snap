// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto" // nolint
	"github.com/rs/zerolog/log"

	pioutil "github.com/amazingchow/photon-dance-snap/ioutil"
	"github.com/amazingchow/photon-dance-snap/snappb"
)

var (
	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	dir string
}

func NewSnapshotter(dir string) *Snapshotter {
	return &Snapshotter{
		dir: dir,
	}
}

func (s *Snapshotter) SaveSnap(snapshot *snappb.Snapshot) error {
	if snapshot.Metadata == nil || snapshot.Metadata.Index == 0 {
		return nil
	}
	return s.save(snapshot)
}

func (s *Snapshotter) save(snapshot *snappb.Snapshot) error {
	start := time.Now()

	fname := fmt.Sprintf("%016x-%016x.snap", snapshot.Metadata.Term, snapshot.Metadata.Index)

	b, err := proto.Marshal(snapshot)
	if err != nil {
		panic(err)
	}
	crc := crc32.Update(0, crcTable, b)
	b, err = proto.Marshal(&snappb.SavedSnapshot{Crc: crc, Data: b})
	if err != nil {
		panic(err)
	}

	spath := filepath.Join(s.dir, fname)

	fsyncStart := time.Now()
	err = pioutil.WriteAndSyncFile(spath, b, 0666)
	snapFsyncSec.Observe(time.Since(fsyncStart).Seconds())

	if err != nil {
		log.Warn().Err(err).Str("path", spath).Msg("failed to write a snap file")
		rerr := os.Remove(spath)
		if rerr != nil {
			log.Warn().Err(err).Str("path", spath).Msg("failed to remove a broken snap file")
		}
		return err
	}

	snapSaveSec.Observe(time.Since(start).Seconds())
	return nil
}

func (s *Snapshotter) Load() (*snappb.Snapshot, error) {
	return s.loadMatched(func(*snappb.Snapshot) bool { return true })
}

func (s *Snapshotter) LoadNewestAvailable(walSnaps []snappb.WalSnapshot) (*snappb.Snapshot, error) {
	return s.loadMatched(func(snapshot *snappb.Snapshot) bool {
		m := snapshot.Metadata
		for i := len(walSnaps) - 1; i >= 0; i-- {
			if m.Term == walSnaps[i].Term && m.Index == walSnaps[i].Index {
				return true
			}
		}
		return false
	})
}

func (s *Snapshotter) loadMatched(matchFn func(*snappb.Snapshot) bool) (*snappb.Snapshot, error) {
	names, err := s.snapnames()
	if err != nil {
		return nil, err
	}
	var snap *snappb.Snapshot
	for _, name := range names {
		if snap, err = loadSnap(s.dir, name); err == nil && matchFn(snap) {
			return snap, nil
		}
	}
	return nil, ErrNoSnapshot
}

func loadSnap(dir, name string) (*snappb.Snapshot, error) {
	fpath := filepath.Join(dir, name)
	snap, err := readSnap(fpath)
	if err != nil {
		log.Warn().Err(err).Str("path", fpath).Msg("failed to read a snap file")
		brokenPath := fpath + ".broken"
		if rerr := os.Rename(fpath, brokenPath); rerr != nil {
			log.Warn().Err(err).Str("path", fpath).Str("broken-path", brokenPath).Msg("failed to rename a broken snap file")
		} else {
			log.Warn().Err(err).Str("path", fpath).Str("broken-path", brokenPath).Msg("renamed to a broken snap file")
		}
	}
	return snap, err
}

func readSnap(snapname string) (*snappb.Snapshot, error) {
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		log.Warn().Err(err).Str("path", snapname).Msg("failed to read a snap file")
		return nil, err
	}
	if len(b) == 0 {
		log.Warn().Str("path", snapname).Msg("failed to read empty snap file")
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.SavedSnapshot
	if err = proto.Unmarshal(b, &serializedSnap); err != nil {
		log.Warn().Str("path", snapname).Msg("failed to unmarshal snappb.SavedSnapshot")
		return nil, err
	}
	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		log.Warn().Str("path", snapname).Msg("failed to read empty snapshot data")
		return nil, ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		log.Warn().Str("path", snapname).Uint32("prev-crc", serializedSnap.Crc).Uint32("new-crc", crc).Msg("snap file is corrupt")
		return nil, ErrCRCMismatch
	}

	var snap snappb.Snapshot
	if err = proto.Unmarshal(serializedSnap.Data, &snap); err != nil {
		log.Warn().Str("path", snapname).Msg("failed to unmarshal snappb.Snapshot")
		return nil, err
	}
	return &snap, nil
}

func (s *Snapshotter) snapnames() ([]string, error) {
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	filenames, err = s.cleanupSnapdir(filenames)
	if err != nil {
		return nil, err
	}
	snaps := checkSuffix(filenames)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(filenames []string) []string {
	snaps := []string{}
	for i := range filenames {
		if strings.HasSuffix(filenames[i], ".snap") {
			snaps = append(snaps, filenames[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[filenames[i]]; !ok {
				log.Warn().Str("path", filenames[i]).Msg("found unexpected non-snap file; skipping")
			}
		}
	}
	return snaps
}

// cleanupSnapdir removes any files that should not be in the snapshot directory:
// - db.tmp prefixed files that can be orphaned by defragmentation
func (s *Snapshotter) cleanupSnapdir(filenames []string) (names []string, err error) {
	names = make([]string, 0, len(filenames))
	for _, filename := range filenames {
		if strings.HasPrefix(filename, "db.tmp") {
			log.Info().Str("path", filename).Msg("found orphaned defragmentation file; deleting")
			if rerr := os.Remove(filepath.Join(s.dir, filename)); rerr != nil && !os.IsNotExist(rerr) {
				return names, fmt.Errorf("failed to remove orphaned .snap.db file %s: %v", filename, rerr)
			}
		} else {
			names = append(names, filename)
		}
	}
	return names, nil
}

func (s *Snapshotter) ReleaseSnapDBs(snap *snappb.Snapshot) error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, filename := range filenames {
		if strings.HasSuffix(filename, ".snap.db") {
			hexIndex := strings.TrimSuffix(filepath.Base(filename), ".snap.db")
			index, err := strconv.ParseUint(hexIndex, 16, 64)
			if err != nil {
				log.Error().Err(err).Str("path", filename).Msg("failed to parse index from snapshot database filename")
				continue
			}
			if index < snap.Metadata.Index {
				log.Info().Str("path", filename).Msg("found orphaned .snap.db file; deleting")
				if rerr := os.Remove(filepath.Join(s.dir, filename)); rerr != nil && !os.IsNotExist(rerr) {
					log.Error().Err(err).Str("path", filename).Msg("failed to remove orphaned .snap.db file")
				}
			}
		}
	}
	return nil
}
