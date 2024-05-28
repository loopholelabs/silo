package main

import (
	"crypto/rand"
	"fmt"
	"time"
)

var somedata = []byte("HELLO")

var big_data = make([]byte, 1*1024*1024*1024)

var little_data = make([]byte, 8192)

func main() {
	READ_DATA := true
	WRITE_DATA := false

	fmt.Printf("Starting example process...\n")

	// randomize some data...
	rand.Read(big_data)
	fmt.Printf("Randomized big data...\n")

	// Now do some things periodically
	ticker := time.NewTicker(time.Second)

	ctime := time.Now()

	n := 0
	for {
		select {
		case <-ticker.C:
			fmt.Printf("Tick %dms %s\n", time.Since(ctime).Milliseconds(), somedata[:1])
			// Toggle somedata between "HELLO" and "WORLD"
			if n == 0 {
				n = 1
				copy(somedata, []byte("WORLD"))
			} else {
				n = 0
				copy(somedata, []byte("HELLO"))
			}

			// Check the data...
			if READ_DATA {
				v := byte(0)
				for _, b := range big_data {
					v += b
				}
				fmt.Printf("SUM %d\n", v)
			}

			if WRITE_DATA {
				// Change some of big_data to see how that interacts with pagefaults...
				zerodata := make([]byte, 12*4096)
				for ptr := 0; ptr < (len(big_data) - len(zerodata)); ptr += 4096 {
					copy(big_data[ptr:], zerodata)
				}
			}

			rand.Read(little_data)
			fmt.Printf("Randomized little data...\n")

		}
	}
}
