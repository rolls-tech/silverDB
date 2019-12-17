package main

import (
	"log"
	"sync"
)

func main() {
	/*var resultCh=make(chan chan int,10)
	var c=make(chan int,0)
	resultCh <-c */
	m:=0
	var wg sync.WaitGroup
	var mu sync.RWMutex
	for i:=0; i<100; i++{
		a:=i
		wg.Add(1)
		go func(n *int,wg *sync.WaitGroup) {
			mu.Lock()
			*n=*n+a
			log.Println(m,a)
			mu.Unlock()
			wg.Done()
		}(&m,&wg)
	}
	wg.Wait()
	log.Println(m)
    /*
    go func() {
   	  	for {
			ch:= <- resultCh
			a:= <-ch
			log.Println(a)
		}
    }()
	c <- m
	log.Println(c)
	time.Sleep(time.Second*3) */
}
