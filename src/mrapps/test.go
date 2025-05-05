package main

import (
	"sync"
)

func main() {
	var wg sync.Cond
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			sendRPC(i)
			wg.Done()
		}()
	}

	wg.Wait()
}

func sendRPC(i int) {
	println(i)
}
