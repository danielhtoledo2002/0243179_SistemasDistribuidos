package log

import (
	"os"
	"testing"

	log_v1 "github.com/miri/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	want := &log_v1.Record{Value: []byte("hello world")}
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.True(t, proto.Equal(want, got))
	}

	// Test reading a non-existent offset
	_, err = s.Read(19) // Assuming we've only written 3 records
	require.Error(t, err)

	// Test closing the segment
	err = s.Close()
	require.NoError(t, err)

	// Test removing the segment
	indexName := s.index.Name()
	storeName := s.store.Name()
	err = s.Remove()
	require.NoError(t, err)

	// Verify that the files have been removed
	_, err = os.Stat(indexName)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(storeName)
	require.True(t, os.IsNotExist(err))
}
