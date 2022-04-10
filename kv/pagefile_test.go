package kv

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestFull(t *testing.T) {
	pf := PageFile{Capacity: 100, PageCount: 99}

	if pf.Full() {
		t.Error("Expected page file not to be full, but was")
	}

	pf.PageCount = 100
	if !pf.Full() {
		t.Error("Expected page file to be full, but was not")
	}
}

func TestInitialize(t *testing.T) {
	pf, path := newPageFile(t)

	// Initialize() is called by newPageFile() already.
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// TODO strictly speaking this test is moot as
			// helper.GetTempFile() will create the file already.
			t.Fatalf("Expected file %s to exist, but did not", path)
		} else {
			t.Fatalf("Unexpected error while accessing page file: %v", err)
		}
	}
	f.Close()

	expected := make([]byte, PageSize)
	expected[3] = 0x05 // Capacity
	// Checksum
	expected[PageSize-4] = 0x7e
	expected[PageSize-3] = 0x13
	expected[PageSize-2] = 0x12
	expected[PageSize-1] = 0xfc

	actual, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Error reading page file: %v", err)
	}

	if !bytes.Equal(actual, expected) {
		t.Errorf("Actual content of page file did not match expected. \nGot %x\nExpected %x", actual, expected)
	}

	_ = pf
}

func TestPageFileEncodeMetaData(t *testing.T) {
	pf := PageFile{
		Capacity:  100,
		PageCount: 4,
		PageLocations: map[PageID]uint32{
			0:  0,
			12: 1,
			42: 2,
			99: 3,
		},
	}

	actual := pf.encodeMetaData()
	expected := make([]byte, PageSize)
	expected[3] = 0x64 // Capacity
	expected[7] = 0x04 // PageCount

	// 0 => 0
	expected[11] = 0x00
	expected[15] = 0x00
	// 12 => 1
	expected[19] = 0x0C
	expected[23] = 0x01
	// 42 => 2
	expected[27] = 0x2A
	expected[31] = 0x02
	// 99 => 3
	expected[35] = 0x63
	expected[39] = 0x03

	// Mind that the range of a map is indeterminate, so we cannot rely on
	// the order of key-value pairs being equal.
	for i := 0; i < 4; i++ {
		expectedStart := 8 + i*8
		expectedPair := expected[expectedStart : expectedStart+8]
		if !bytes.Contains(actual[8:40], expectedPair) {
			t.Errorf("Expected encoded metadata to contain key-value pair %x", expectedPair)
		}
	}

	// We cannot check the checksum, as the order of key/value pairs might
	// be different in the encoding from ours.
}

func TestPageFileDecodeMetaData(t *testing.T) {
	pf := PageFile{}

	metaData := make([]byte, PageSize)
	metaData[3] = 0x64 // Capacity
	metaData[7] = 0x05 // PageCount

	// 0 => 0
	metaData[11] = 0x00
	metaData[15] = 0x00
	// 12 => 1
	metaData[19] = 0x0C
	metaData[23] = 0x01
	// 42 => 2
	metaData[27] = 0x2A
	metaData[31] = 0x02
	// 99 => 3
	metaData[35] = 0x63
	metaData[39] = 0x03

	// Checksum
	metaData[PageSize-4] = 0x56
	metaData[PageSize-3] = 0xe7
	metaData[PageSize-2] = 0x46
	metaData[PageSize-1] = 0x09

	err := pf.decodeMetaData(metaData)
	if err != nil {
		t.Fatalf("Error decoding meta data: %v", err)
	}

	if pf.Capacity != 100 {
		t.Errorf("Got unexpected capacity %d; expected 100", pf.Capacity)
	}

	if pf.PageCount != 5 {
		t.Errorf("Got unexpected page count %d; expected 5", pf.PageCount)
	}

	tests := []struct {
		key   PageID
		value uint32
	}{
		{0, 0},
		{12, 1},
		{42, 2},
		{99, 3},
	}

	for _, test := range tests {
		v, ok := pf.PageLocations[test.key]
		if !ok {
			t.Errorf("Key %d did not exist in page location map; but should have", test.key)
		}

		if v != test.value {
			t.Errorf("Key %d had value %d in page location map; but should have value %d", test.key, v, test.value)
		}
	}
}

func newPageFile(t *testing.T) (*PageFile, string) {
	file := helper.GetTempFile(t, "pagefile")

	pf := PageFile{
		Path:     file,
		Capacity: 5,
	}

	err := pf.Initialize()
	if err != nil {
		t.Errorf("Unable to initialize page file: %v", err)
	}

	return &pf, file
}
