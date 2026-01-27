package db

import (
	"encoding/json"
	"fmt"
	"os"
)

type DB interface {
	Set(key, value string)
	Get(key string) string
	Delete(key string)
}

type db struct {
	store map[string]string
	file  *os.File
}

func NewDB(dirLocation string) (DB, error) {
	file, err := os.OpenFile("./my-db.txt", os.O_RDWR, 0644)
	if err != nil {
		newFile, err := os.Create("my-db.txt")

		if err != nil {
			return nil, fmt.Errorf("failed creating new db file: %s", err)
		}

		defer newFile.Close()
		return NewDB(dirLocation) // Try agian.
	}

	return &db{
		store: make(map[string]string),
		file:  file,
	}, nil
}

func (d *db) Set(key, value string) {
	d.store[key] = value
	d.save()
}

func (d *db) Get(key string) string {
	return d.store[key]
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

	fmt.Printf("here?")
}
