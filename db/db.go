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
	startQueueHandler()
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
		d.set(v[0], v[1])
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

func (d *db) Get(key string) (string, error) {
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
