package kv

import "testing"

const diskSize = 8

func emptyDisks() []Disk {
	return []Disk{
		NewRAMDisk(diskSize, diskSize),
	}
}

func TestDisk_AllocatePage(t *testing.T) {
	disks := emptyDisks()

	for _, disk := range disks {
		for i := uint32(0); i < disk.Capacity(); i++ {
			page, err := disk.AllocatePage()

			if err != nil {
				t.Errorf("Actual error = %s, Expected == nil", err)
			}
			if page.id != PageID(i) {
				t.Errorf("Actual PageID = %d, Expected == %d", page.id, i)
			}
			if disk.Occupied() != i+1 {
				t.Errorf("Actual occupied = %d, Expected == %d", disk.Occupied(), i)
			}
		}

		for i := 0; i < 4; i++ {
			_, err := disk.AllocatePage()

			if err == nil {
				t.Errorf("Actual error = nil, Expected == \"unable to allocate page on RAM disk\"")
			}
			if disk.Occupied() != disk.Capacity() {
				t.Errorf("Actual occupied = %d, Expected == %d", disk.Occupied(), i)
			}
		}
	}
}

func TestDisk_DeallocatePage(t *testing.T) {
	disks := emptyDisks()

	for _, disk := range disks {
		for i := uint32(0); i < disk.Capacity(); i++ {
			for j := i; j < disk.Capacity(); j++ {
				page, _ := disk.AllocatePage()
				if page.id != PageID(i) {
					t.Errorf("Actual page = %d, Expected == %d", page.id, i)
				}
				disk.DeallocatePage(page.id)
				if disk.Occupied() != i {
					t.Errorf("Actual occupied = %d, Expected == %d", disk.Occupied(), i)
				}
			}
			_, _ = disk.AllocatePage()
		}
	}
}

func TestDisk_ReadPage(t *testing.T) {
	disks := emptyDisks()

	for _, disk := range disks {
		for i := uint32(0); i < disk.Capacity(); i++ {
			newPage, _ := disk.AllocatePage()
			page, err := disk.ReadPage(newPage.id)
			if err != nil {
				t.Errorf("Actual error = %s, Expected == nil", err)
			}

			if newPage != page {
				t.Errorf("Actual retrieved page = %x, Expected == %x", &page, &newPage)
			}
		}
	}
}

func FuzzDisk_WritePage(f *testing.F) {
	f.Add([]byte{42, 69})
	f.Fuzz(func(t *testing.T, in []byte) {
		disks := emptyDisks()

		for _, disk := range disks {
			newPage, _ := disk.AllocatePage()
			copy(newPage.data[:], in)

			if err := disk.WritePage(newPage); err != nil {
				t.Errorf("Actual error = %s, Expected == nil", err)
			}

			page, _ := disk.ReadPage(newPage.id)
			if page.data != newPage.data {
				t.Errorf("Actual data = %x, Expected == %x", page.data, newPage.data)
			}
		}
	})
}
