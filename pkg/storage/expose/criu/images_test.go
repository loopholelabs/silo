package criu

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func readImages(t *testing.T, dir string) (map[uint32]map[uint64][]byte, map[uint32]map[uint64]string, map[uint32]map[uint64]uint32) {
	page_data := make(map[uint32]map[uint64][]byte)
	page_data_hashes := make(map[uint32]map[uint64]string)
	page_flags := make(map[uint32]map[uint64]uint32)

	cb := func(pid uint32, address uint64, data []byte, flags uint32) {
		maps, ok := page_data[pid]
		if !ok {
			maps = make(map[uint64][]byte)
			page_data[pid] = maps
			page_data_hashes[pid] = make(map[uint64]string)
			page_flags[pid] = make(map[uint64]uint32)
		}

		maps[address] = data

		hasher := sha256.New()
		hasher.Write(data)
		page_data_hashes[pid][address] = hex.EncodeToString(hasher.Sum(nil))

		page_flags[pid][address] = flags
	}

	err := Import_images(dir, cb)
	assert.NoError(t, err)
	return page_data, page_data_hashes, page_flags
}

func TestReadImages(t *testing.T) {
	_, page_data_hashes, page_flags := readImages(t, "testdata/images_1")

	// Check the data is all correct...
	expected_flags := map[uint32]map[uint64]uint32{0x52873: map[uint64]uint32{
		0x645427ef0000: 0x4,
		0x645427f0a000: 0x4,
		0x645427f0e000: 0x2,
		0x645429875000: 0x2,
		0x73551c9fa000: 0x4,
		0x73551ca00000: 0x2,
		0x73551ca04000: 0x2,
		0x73551ca0c000: 0x2,
		0x73551cb9e000: 0x2,
		0x73551cbb7000: 0x2,
		0x73551cbee000: 0x4,
		0x7ffcb5362000: 0x2,
		0x7ffcb5385000: 0x4}, 0x528f2: map[uint64]uint32{
		0x55aadef4a000: 0x4,
		0x55aadef4f000: 0x4,
		0x55aae06e2000: 0x2,
		0x721c381fa000: 0x4,
		0x721c38200000: 0x2,
		0x721c38204000: 0x2,
		0x721c3820c000: 0x2,
		0x721c382a8000: 0x2,
		0x721c382c1000: 0x2,
		0x721c382f8000: 0x4,
		0x7fff6007f000: 0x2,
		0x7fff60162000: 0x4}}

	assert.Equal(t, expected_flags, page_flags)

	// Check the hashed the page data
	expected_hashes := map[uint32]map[uint64]string{
		0x52873: map[uint64]string{
			0x645427ef0000: "f4698a999f8239624fc5ac01a1ee8dbf77b53c04022316aa154412c608be79f6",
			0x645427f0a000: "2518e6b1a87d09a562d74a71b47fddbee6e82f0228c290577dfd5278d1cca595",
			0x645427f0e000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x645429875000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x73551c9fa000: "a564ead475f3348057c6f52d990aed0d955825aef7e358b85584d4acda1afe25",
			0x73551ca00000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x73551ca04000: "f3cc103136423a57975750907ebc1d367e2985ac6338976d4d5a439f50323f4a",
			0x73551ca0c000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x73551cb9e000: "9f1dcbc35c350d6027f98be0f5c8b43b42ca52b7604459c0c42be3aa88913d47",
			0x73551cbb7000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x73551cbee000: "f2537f3529ca476d3f9b5ff79a7d66d225b33f6f36d557f4d4d11dd9e6296d2f",
			0x7ffcb5362000: "f3cc103136423a57975750907ebc1d367e2985ac6338976d4d5a439f50323f4a",
			0x7ffcb5385000: "bf6ce2c00fd2f7ef9b942a3faf51929f1ceb926d9b62611b4b38437cace67044"},
		0x528f2: map[uint64]string{
			0x55aadef4a000: "992ca1265a85645d06d421873b0b10fc2d88a4f79e8a7440bed3bf9caec058eb",
			0x55aadef4f000: "8bf84367ad70294b598b5ff621430a498bfd6957f60542a06b8191f6db4eb33a",
			0x55aae06e2000: "9f1dcbc35c350d6027f98be0f5c8b43b42ca52b7604459c0c42be3aa88913d47",
			0x721c381fa000: "e851c37d71789dd3dd64f2196780f45a34c2e1b2f1c151bf9b90d2a4fdbef0d0",
			0x721c38200000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x721c38204000: "f3cc103136423a57975750907ebc1d367e2985ac6338976d4d5a439f50323f4a",
			0x721c3820c000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x721c382a8000: "9f1dcbc35c350d6027f98be0f5c8b43b42ca52b7604459c0c42be3aa88913d47",
			0x721c382c1000: "ad7facb2586fc6e966c004d7d1d16b024f5805ff7cb47c7a85dabd8b48892ca7",
			0x721c382f8000: "0e34c6b26bc915aaa62d552a1fc63f2a91fc6e96f1d4cb5ccd993b8b4c4b0bb6",
			0x7fff6007f000: "4fe7b59af6de3b665b67788cc2f99892ab827efae3a467342b3bb4e3bc8e5bfe",
			0x7fff60162000: "bf6ce2c00fd2f7ef9b942a3faf51929f1ceb926d9b62611b4b38437cace67044"}}

	assert.Equal(t, expected_hashes, page_data_hashes)
}

func TestWriteImages(t *testing.T) {
	page_data, _, page_flags := readImages(t, "testdata/images_2")

	// Now remove any LAZY data, and write it out. Then read it back and verify it's as expected...

	os.RemoveAll("testdata/images_tmp")
	os.Mkdir("testdata/images_tmp", 0777)

	t.Cleanup(func() {
		os.RemoveAll("testdata/images_tmp")
	})

	// Remove any lazy pages that are present, and export to images
	for pid, s := range page_flags {
		for addr, flag := range s {
			if (flag & PE_LAZY) == PE_LAZY {
				s[addr] = s[addr] & ^uint32(PE_PRESENT)
			}
		}
		err := Export_image(fmt.Sprintf("testdata/images_tmp/pagemap-%d.img", pid), page_data[pid], s)
		assert.NoError(t, err)
	}

	// Now read it back and check it's as expected...
	_, page_data_hashes_ref, page_flags_ref := readImages(t, "testdata/images_1")
	_, page_data_hashes_tmp, page_flags_tmp := readImages(t, "testdata/images_tmp")

	assert.Equal(t, page_data_hashes_ref, page_data_hashes_tmp)
	assert.Equal(t, page_flags_ref, page_flags_tmp)
}
