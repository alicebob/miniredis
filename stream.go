package miniredis

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Basic stream implementation.

var (
	errInvalidStreamIDFormat = errors.New("ERR Invalid stream ID specified as stream command argument")
)

type streamKey []streamEntry

type streamEntry struct {
	id     streamEntryID
	values [][2]string
}

type streamEntryID [2]uint64

func (id *streamEntryID) Less(other streamEntryID) bool {
	if other[0] > id[0] {
		return true
	}

	if other[0] == id[0] {
		return other[1] > id[1]
	}

	return false
}

func (id *streamEntryID) String() string {
	return fmt.Sprintf("%d-%d", id[0], id[1])
}

func newStream() streamKey {
	return streamKey{}
}

func (ss *streamKey) append(id streamEntryID, values [][2]string) {
	*ss = append(*ss, streamEntry{id: id, values: values})
}

func (ss *streamKey) nextEntryID(now time.Time) streamEntryID {
	curTime := uint64(now.UnixNano()) / 1000

	lastID := ss.getLastEntryID()

	if lastID[0] < curTime {
		return streamEntryID{curTime, 0}
	}

	return streamEntryID{lastID[0], lastID[1] + 1}
}

func (ss *streamKey) isValidNextEntryID(next streamEntryID) error {
	last := ss.getLastEntryID()

	if !last.Less(next) {
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return nil
}

func (ss *streamKey) getLastEntryID() streamEntryID {
	// Return a zero value in case there is no entry
	// Note that deleted entries will also need to be tracked
	if len(*ss) == 0 {
		return streamEntryID{}
	}

	return (*ss)[len(*ss)-1].id
}

func formatStreamEntryID(id string) (fmtid streamEntryID, err error) {
	parts := strings.Split(id, "-")
	if len(parts) != 1 && len(parts) != 2 {
		return fmtid, errInvalidStreamIDFormat
	}

	ts, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return fmtid, errInvalidStreamIDFormat
	}

	var seq uint64
	if len(parts) == 2 {
		seq, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return fmtid, errInvalidStreamIDFormat
		}
	}

	if ts == 0 && seq == 0 {
		return fmtid, errInvalidStreamIDFormat
	}

	return streamEntryID{ts, seq}, nil
}

func formatStreamRangeBound(id string, start bool, reverse bool) (fmtid streamEntryID, err error) {
	if id == "-" {
		return streamEntryID{0, 0}, nil
	}

	if id == "+" {
		return streamEntryID{math.MaxUint64, math.MaxUint64}, nil
	}

	if id == "0" {
		return streamEntryID{0, 0}, nil
	}

	parts := strings.Split(id, "-")
	if len(parts) == 2 {
		return formatStreamEntryID(id)
	}

	// Incomplete IDs case
	ts, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return fmtid, errInvalidStreamIDFormat
	}

	if (!start && !reverse) || (start && reverse) {
		return streamEntryID{ts, math.MaxUint64}, nil
	}

	return streamEntryID{ts, 0}, nil
}

func reversedStreamEntries(o []streamEntry) []streamEntry {
	newStream := make([]streamEntry, len(o))

	for i, e := range o {
		newStream[len(o)-i-1] = e
	}

	return newStream
}
