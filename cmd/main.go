package main

import "fmt"

func main() {
	err := Execute()
	if err != nil && err.Error() != "" {
		fmt.Println(err)
	}
}
