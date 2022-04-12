package kv

import "bytes"

// PageSize is the default page size of a whole page.
const PageSize = 4096

// const PageSize = 64

// PageMetadataSize is the size of the page metadata. Equivalent to the starting index of page data.
const PageMetadataSize = 7

// PageDataSize is the buffer size for data to be stored in a Page.
const PageDataSize = PageSize - PageMetadataSize

type PageID uint32

/*
Page is a fixed-length block of PageSize that contains some bytes of metadata and a large data buffer of PageDataSize.
*/
type Page struct {
	// id of the page.
	id PageID
	// pinCount tracks the number of concurrent accesses.
	pinCount uint16
	// isDirty indicates the page was modified after being read.
	isDirty bool
	// data stores the raw node data.
	data [PageDataSize]byte
}

// decrementPinCount decrements the pin count unless it was 0 already.
func (p *Page) decrementPinCount() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

// Equal compares two pages for equality.
//
// Two pages are considered equal only if all their fields including the data
// slice are equal.
func (p *Page) Equal(other *Page) bool {
	if p.id != other.id {
		return false
	}

	if p.pinCount != other.pinCount {
		return false
	}

	if p.isDirty != other.isDirty {
		return false
	}

	if !bytes.Equal(p.data[:], other.data[:]) {
		return false
	}

	return true
}
