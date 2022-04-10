package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
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

func TestReadAndWritePage(t *testing.T) {
	pf, _ := newPageFile(t)

	page1 := &Page{
		id:   0,
		data: [PageDataSize]byte{0x21, 0x30, 0xA0, 0xFB},
	}

	page2 := &Page{
		id:   42,
		data: [PageDataSize]byte{0x00, 0x2},
	}

	err := pf.WritePage(page1)
	if err != nil {
		t.Fatalf("Error write page %d: %v", page1.id, err)
	}
	if pf.PageCount != 1 {
		t.Errorf("Expected page file to have page count 1; got %d", pf.PageCount)
	}

	err = pf.WritePage(page2)
	if err != nil {
		t.Fatalf("Error write page %d: %v", page2.id, err)
	}
	if pf.PageCount != 2 {
		t.Errorf("Expected page file to have page count 2; got %d", pf.PageCount)
	}

	p1Read, err := pf.ReadPage(page1.id)
	if err != nil {
		t.Fatalf("Error reading page %d: %v", page1.id, err)
	}

	p2Read, err := pf.ReadPage(page2.id)
	if err != nil {
		t.Fatalf("Error reading page %d: %v", page2.id, err)
	}

	if !comparePage(page1, p1Read) {
		t.Errorf(
			"Got unexpected page when reading page %d.\n Got %+v\nExpected %+v",
			page1.id,
			p1Read,
			page1,
		)
	}

	if !comparePage(page2, p2Read) {
		t.Errorf(
			"Got unexpected page when reading page %d.\n Got %+v\nExpected %+v",
			page2.id,
			p2Read,
			page2,
		)
	}
}

func TestWriteNewPagePersistsMetadata(t *testing.T) {
	pf, _ := newPageFile(t)

	page1 := &Page{
		id:   0,
		data: [PageDataSize]byte{0x21, 0x30, 0xA0, 0xFB},
	}

	page2 := &Page{
		id:   42,
		data: [PageDataSize]byte{0x00, 0x2},
	}

	err := pf.WritePage(page1)
	if err != nil {
		t.Fatalf("Error write page %d: %v", page1.id, err)
	}

	err = pf.WritePage(page2)
	if err != nil {
		t.Fatalf("Error write page %d: %v", page2.id, err)
	}

	// Now we'll reinitialize and make sure that it all still works
	err = pf.Initialize()
	if err != nil {
		t.Fatalf("Error reinitializing page file: %v", err)
	}

	p1Read, err := pf.ReadPage(page1.id)
	if err != nil {
		t.Fatalf("Error reading page %d: %v", page1.id, err)
	}

	p2Read, err := pf.ReadPage(page2.id)
	if err != nil {
		t.Fatalf("Error reading page %d: %v", page2.id, err)
	}

	if !comparePage(page1, p1Read) {
		t.Errorf(
			"Got unexpected page when reading page %d.\n Got %+v\nExpected %+v",
			page1.id,
			p1Read,
			page1,
		)
	}

	if !comparePage(page2, p2Read) {
		t.Errorf(
			"Got unexpected page when reading page %d.\n Got %+v\nExpected %+v",
			page2.id,
			p2Read,
			page2,
		)
	}
}

func comparePage(a, b *Page) bool {
	return a.id == b.id && a.isDirty == b.isDirty && a.pinCount == b.pinCount && a.data == b.data
}

func TestInitialize(t *testing.T) {
	// So we can actually sensibly hardcode some values below
	pf, path := newPageFileWithCapacity(t, 5)

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

	// Checksum must be calculated dynamically, as it will depend on the page size
	checksum := crc32.ChecksumIEEE(expected[:PageSize-4])
	binary.BigEndian.PutUint32(expected[PageSize-4:], checksum)

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

	// Checksum must be calculated dynamically, as it will depend on the page size
	checksum := crc32.ChecksumIEEE(metaData[:PageSize-4])
	binary.BigEndian.PutUint32(metaData[PageSize-4:], checksum)

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

func newPageFileWithCapacity(t *testing.T, capacity uint32) (*PageFile, string) {
	file := helper.GetTempFile(t, "pagefile")

	pf := PageFile{
		Path:     file,
		Capacity: capacity,
	}

	// TODO this is a bit ugly. PageFile requires the file to *not* be
	// present to trigger automatic initialization of the file. However
	// helper.GetTempFile() *creates* the file.
	//
	// We work around this by deleting it, which is awful and could be
	// subject to an (unlikely) race condition.
	err := os.Remove(file)
	if err != nil {
		t.Fatalf("Unable to remove temporary file: %v", err)
	}

	// Initialize the page file
	err = pf.Initialize()
	if err != nil {
		t.Fatalf("Unable to initialize page file: %v", err)
	}

	return &pf, file
}

func newPageFile(t *testing.T) (*PageFile, string) {
	capacity := (PageSize - 12) / 8 // To make sure meta data fits in one page
	return newPageFileWithCapacity(t, uint32(capacity))
}
