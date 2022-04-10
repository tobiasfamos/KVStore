package kv

type PersistentDisk struct {
	Directory string
}

func NewPersistentDisk(directory string) (Disk, error) {
	d := PersistentDisk{
		Directory: directory,
	}

	return &d, nil
}

func (d *PersistentDisk) AllocatePage() (*Page, error) {
	return &Page{}, nil
}

func (d *PersistentDisk) DeallocatePage(id PageID) {
}

func (d *PersistentDisk) ReadPage(id PageID) (*Page, error) {
	return &Page{}, nil
}

func (d *PersistentDisk) WritePage(page *Page) error {
	return nil
}

func (d *PersistentDisk) Occupied() uint {
	return 42
}

func (d *PersistentDisk) Capacity() uint {
	return 42
}
