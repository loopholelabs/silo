/*
   Copyright (C) 2024 Loophole Labs

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func main() {
	pageSize := os.Getpagesize()
	smallSize := pageSize * 1024
	xlSize := smallSize * 1024

	fmt.Printf("using page size %d bytes with small size %d bytes (%d mB) and XL size %d bytes (%d mB)\n",
		pageSize, smallSize, smallSize/1024/1024, xlSize, xlSize/1024/1024)

	files := []struct {
		name string
		size int
	}{
		{
			name: "base.bin",
			size: smallSize,
		},
		{
			name: "base2.bin",
			size: smallSize,
		},
		{
			name: "overlay.bin",
			size: smallSize,
		},
		{
			name: "baseXL.bin",
			size: xlSize,
		},
		{
			name: "baseXL2.bin",
			size: xlSize,
		},
		{
			name: "overlayXL.bin",
			size: xlSize,
		},
	}

	for _, f := range files {
		fmt.Printf("creating '%s'\n", f.name)
		out, err := os.OpenFile(f.name, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer out.Close()

		in, err := os.Open("/dev/random")
		if err != nil {
			panic(err)
		}
		defer in.Close()

		if _, err := io.CopyN(out, in, int64(f.size)); err != nil {
			panic(err)
		}
	}

	fmt.Println("creating 'overlay_zebra.bin'")
	{
		out, err := os.OpenFile("overlay_zebra.bin", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer out.Close()

		bufferSize := pageSize

		var one uint8 = 0xFF
		var zero uint8 = 0x00
		ones := make([]byte, bufferSize)
		zeros := make([]byte, bufferSize)
		for i := 0; i < bufferSize; i++ {
			ones[i] = one
			zeros[i] = zero
		}

		for i := 0; i < smallSize/bufferSize; i++ {
			if i%2 == 0 {
				err = binary.Write(out, binary.LittleEndian, zeros)
				if err != nil {
					panic(err)
				}
			} else {
				err = binary.Write(out, binary.LittleEndian, ones)
				if err != nil {
					panic(err)
				}
			}
		}
	}
}
