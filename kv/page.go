package kv

// PageSize is the default page size of a whole page.
const PageSize = 4096

// PageMetadataSize is the size of the page metadata. Equivalent to the starting index of page data.
const PageMetadataSize = 8

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
	// isLeaf indicates the page to be either a LeafNode or an InternalNode.
	isLeaf bool
	// data stores the raw node data.
	data [PageDataSize]byte
}

// decrementPinCount decrements the pin count unless it was 0 already.
func (p *Page) decrementPinCount() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}
