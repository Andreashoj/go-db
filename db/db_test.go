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
	DB.Sync()

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
	DB.Sync()
}

func TestDb_Find(t *testing.T) {
	DB := freshDB()

	// Seed
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%v", i)
		value := fmt.Sprintf("{\"value\": \"value-%v\"}", i)
		DB.Set(key, value)
	}

	DB.Sync()

	for i := 0; i < 10; i++ {
		val := fmt.Sprintf("value-%v", i)

		value, err := DB.Find(func(item map[string]interface{}) bool {
			return item["value"] == val
		})

		if err != nil {
			t.Errorf("didn't expect an error here: %s", err)
		}

		if value["value"] != val {
			t.Errorf("expected %s to be equal %s", value["value"], val)
		}
	}
}

func TestDb_FindShouldFail(t *testing.T) {
	DB := freshDB()

	// Seed
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%v", i)
		value := fmt.Sprintf("{\"value\": \"value-%v\"}", i)
		DB.Set(key, value)
	}

	DB.Sync()

	for i := 11; i < 21; i++ {
		val := fmt.Sprintf("value-%v", i)

		value, err := DB.Find(func(item map[string]interface{}) bool {
			return item["value"] == val
		})

		if err == nil {
			t.Errorf("expected the find to fail, instead got value %s", value)
		}
	}
}

func TestDb_Where(t *testing.T) {
	DB := freshDB()

	// Seed
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%v", i)
		value := fmt.Sprintf("{\"value\": %v}", i)
		DB.Set(key, value)
	}

	DB.Sync()

	items, err := DB.Where(func(item map[string]interface{}) bool {
		return int(item["value"].(float64))%2 == 0
	})

	if err != nil {
		t.Errorf("didn't expect an error here: %s", err)
	}

	if len(items) != 50 {
		t.Errorf("expected length to be 50 but got %v", len(items))
	}
}

func TestDb_WhereShouldReturnEmpty(t *testing.T) {
	DB := freshDB()

	// Seed
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%v", i)
		value := fmt.Sprintf("{\"value\": %v}", i)
		DB.Set(key, value)
	}

	DB.Sync()

	items, err := DB.Where(func(item map[string]interface{}) bool {
		return int(item["value"].(float64)) > 100
	})

	if err != nil {
		t.Errorf("didn't expect an error here: %s", err)
	}

	if len(items) != 0 {
		t.Errorf("expected length to be 0 but got %v", len(items))
	}
}
