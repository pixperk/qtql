package main

import "time"

type Row struct {
	Key   key
	Value value
}

// volcano style operator
// what it does is defined by the Next() method
type Operator interface {
	Open() error
	Next() (*Row, error)
	Close() error
}

type Filter struct {
	Input Operator
	//the predicate is a function that returns true if the row should be kept
	Pred func(row Row) bool
}

// like a table scan operator
type KVScan struct {
	store *Store
	keys  []key
	pos   int
}

func NewKVScan(store *Store) *KVScan {
	return &KVScan{store: store}
}

// open collects all valid keys at the time of opening
// it sets the position to 0
// it locks the store for reading but unlocks it in Close
// this way, the scan will only see the keys that were valid at the time of opening
func (kv *KVScan) Open() error {
	kv.store.lock.RLock()

	kv.keys = make([]key, 0, len(kv.store.data))
	now := time.Now()
	for k, v := range kv.store.data {
		if v.expires_at.IsZero() || v.expires_at.After(now) {
			kv.keys = append(kv.keys, k)
		}
	}
	kv.pos = 0
	return nil
}

// Next returns the next valid row or nil if there are no more rows
func (kv *KVScan) Next() (*Row, error) {
	if kv.pos >= len(kv.keys) {
		return nil, nil // Indicate end of data
	}

	key := kv.keys[kv.pos]
	value := kv.store.data[key]
	kv.pos++

	return &Row{Key: key, Value: value}, nil
}

// Close unlocks the store
func (kv *KVScan) Close() error {
	kv.store.lock.RUnlock()
	return nil
}

func (f *Filter) Open() error {
	return f.Input.Open()
}

// Next returns the next row that satisfies the predicate
func (f *Filter) Next() (*Row, error) {
	for {
		row, err := f.Input.Next()
		if err != nil || row == nil {
			return nil, err
		}
		if f.Pred(*row) {
			return row, nil
		}
	}
}

func (f *Filter) Close() error {
	return f.Input.Close()
}
