package db

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestDb_Set_RaceConditions(t *testing.T) {
	// Should test race conditions primarily
	DB, err := NewDB("../test/test-db.txt")
	if err != nil {
		t.Errorf("failed creating db: %s", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		go wg.Go(func() {
			DB.Set(fmt.Sprintf("test-val-%v", i), strconv.Itoa(i))
		})
	}

	wg.Wait()

	for i := 0; i < 100; i++ {
		val, _ := DB.Get(fmt.Sprintf("test-val-%v", i))
		if val != strconv.Itoa(i) {
			t.Errorf("expected %s to be equal %s", val, strconv.Itoa(i))
		}
	}
}

func freshDB() DB {

	db, _ := NewDB("../test/test-db.txt")
	return db
}
func TestDb_Set_Get_RaceConditions(t *testing.T) {
	// Should ensure that no partial reads happen due to race conditions
	DB, err := NewDB("../test/test-db.txt")
	if err != nil {
		t.Errorf("failed creating db: %s", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		go func() {
			wg.Add(1)
			_, err := DB.Get(fmt.Sprintf("test-val-%v", i))
			if err == nil {
				t.Errorf("did not expect to have read value for key: %s", fmt.Sprintf("test-val-%v", i))
			}
			wg.Done()
		}()

		go func() {
			wg.Add(1)
			DB.Set(fmt.Sprintf("test-val-%v", i), strconv.Itoa(i))
			wg.Done()
		}()
	}

	wg.Wait()
}
