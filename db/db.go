package db

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type DB interface {
	Set(key, value string)
	Get(key string) (string, error)
	Delete(key string)
	Find(handler func(item map[string]interface{}) bool) (map[string]interface{}, error)
	Where(handler func(item map[string]interface{}) bool) ([]map[string]interface{}, error)
	startQueueHandler()
	clear()
	Sync()
}

type db struct {
	store    map[string]string
	file     *os.File
	mu       sync.Mutex
	requests chan []string
	wg       sync.WaitGroup
}

func NewDB(filePath string) (DB, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		fallback := fmt.Sprintf("%s/my-db.txt", filePath)
		newFile, err := os.Create(fallback)

		if err != nil {
			return nil, fmt.Errorf("failed creating new db file: %s", err)
		}

		defer newFile.Close()
		return NewDB(fallback) // Try agian.
	}

	database := db{
		store:    make(map[string]string),
		file:     file,
		requests: make(chan []string), // Should be specific to type of requests, only writes needs to be "queued"
	}

	// Initialize request handler
	go database.startQueueHandler()

	return &database, nil
}

func (d *db) startQueueHandler() {
	for v := range d.requests {
		d.mu.Lock()
		key, val := v[0], v[1]
		d.set(key, val)
		d.wg.Done()
		d.mu.Unlock()
	}
}

func (d *db) Set(key, value string) {
	d.wg.Add(1)
	d.requests <- []string{key, value}
}

func (d *db) set(key, value string) {
	d.store[key] = value
	d.save()
}

func (d *db) Get(key string) (string, error) { // TODO: Allow multiple reads to happen at once
	d.mu.Lock()
	defer d.mu.Unlock()

	value, exists := d.store[key]
	if !exists {
		return "", fmt.Errorf("couldn't find value for key %s", key)
	}

	return value, nil
}

func (d *db) Delete(key string) {
	delete(d.store, key)
	d.save()
}

func (d *db) Where(handler func(item map[string]interface{}) bool) ([]map[string]interface{}, error) {
	var matches []map[string]interface{}
	for k, v := range d.store {
		var doc map[string]interface{}
		err := json.Unmarshal([]byte(v), &doc)
		if err != nil {
			return nil, fmt.Errorf("failed decoding value %s for key %s with error %s", v, k, err)
		}

		if handler(doc) {
			matches = append(matches, doc)
		}
	}

	return matches, nil
}

func (d *db) Find(handler func(item map[string]interface{}) bool) (map[string]interface{}, error) {
	for key, value := range d.store {
		var mappedItem interface{}
		err := json.Unmarshal([]byte(value), &mappedItem)
		docMap := mappedItem.(map[string]interface{})
		if err != nil {
			return nil, fmt.Errorf("failed decoding entry %s with error :%s", key, err)
		}

		if handler(docMap) {
			return docMap, nil
		}
	}

	return nil, fmt.Errorf("couldn't find the entry you were looking for")
}

func (d *db) save() {
	JSON, err := json.Marshal(d.store)
	if err != nil {
		fmt.Printf("failed encoding store to json: %s", err)
		return
	}

	d.file.Seek(0, 0)
	d.file.Truncate(0)
	d.file.Write(JSON)
	d.file.Sync()
}

// TODO: Maybe create another db struct for testing specifically ?
func (d *db) clear() {
	d.file.Seek(0, 0)
	d.file.Truncate(0)
	d.file.Write([]byte(""))
	d.file.Sync()
}

func (d *db) Sync() {
	d.wg.Wait()
}
