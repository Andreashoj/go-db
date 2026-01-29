package db

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func freshDB() DB {
	db, _ := NewDB("../test/test-db.txt")
	db.clear()
	return db
}

func TestDb_Set_Ordering(t *testing.T) {
	DB := freshDB()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			DB.Set(fmt.Sprintf("test-val-%v", i), strconv.Itoa(i))
		}()
	}

	wg.Wait()

	for i := 0; i < 100; i++ {
		val, _ := DB.Get(fmt.Sprintf("test-val-%v", i))
		if val != strconv.Itoa(i) {
			t.Errorf("expected %s to be equal %s", val, strconv.Itoa(i))
		}
	}
}

func TestDb_ValidData(t *testing.T) {
	DB := freshDB()

	// Seed data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%v", i)
		val := fmt.Sprintf("my-long-value-%v", i)
		DB.Set(key, val)
	}
	DB.wait()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		key := fmt.Sprintf("key-%v", i)
		val := fmt.Sprintf("my-long-value-%v", i)

		go func() {
			defer wg.Done()
			dbVal, err := DB.Get(key)
			if err != nil {
				t.Errorf("failed retrieving value for key %s", key)
			}

			if !strings.Contains(dbVal, "my-long-value-") {
				t.Errorf("val did not contain the expected value %s, got %s", val, dbVal)
			}
		}()

		go func() {
			longerVal := fmt.Sprintf("my-long-value-%v-that-is-super-cool", i)
			DB.Set(key, longerVal)
		}()
	}

	wg.Wait()
	DB.wait()
}
