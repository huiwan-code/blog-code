package main

import (
	"container/list"
	"errors"
	"sync"
)

type Data struct {
	Key 	string
	Val     interface{}
}

type LruCache struct {
	sync.RWMutex
	list 		*list.List
	table 		map[string]*list.Element
	capacity	int
	size 		int
}

func (lru *LruCache) get(key string) (interface{}, error) {
	if el, ok := lru.table[key]; ok {
		data, _ := el.Value.(*Data)
		return data.Val, nil
	} else {
		return nil, errors.New("key not exist")
	}
}

func (lru *LruCache) put(key string, val interface{})  {
	lru.Lock()
	defer lru.Unlock()
	if _, ok := lru.table[key]; ok {
		data, _ := lru.table[key].Value.(*Data)
		data.Val = val
		lru.list.MoveToFront(lru.table[key])
	} else {
		if lru.size >= lru.capacity {
			lru.RemoveLast()
		}
		lru.list.PushFront( &Data{
			key, val,
		})
		lru.table[key] = lru.list.Front()
		lru.size ++
	}
}

func (lru *LruCache) RemoveLast() {
	el := lru.list.Back()
	data ,_ := el.Value.(*Data)
	delete(lru.table, data.Key)
	lru.list.Remove(el)
	lru.size --
}



