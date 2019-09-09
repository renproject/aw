package testutil

import "github.com/renproject/kv"

type FaultyTable struct {
	GetErr    error
	InsertErr error
	DeleteErr error
	SizeErr   error
}

func NewFaultyTable(getErr, insertErr, deleteErr, sizeErr error) *FaultyTable {
	return &FaultyTable{
		GetErr:    getErr,
		InsertErr: insertErr,
		DeleteErr: deleteErr,
		SizeErr:   sizeErr,
	}
}

func (table *FaultyTable) Get(key string, value interface{}) error {
	return table.GetErr
}

func (table *FaultyTable) Insert(key string, value interface{}) error {
	return table.InsertErr
}

func (table *FaultyTable) Delete(key string) error {
	return table.DeleteErr
}

func (table *FaultyTable) Iterator() kv.Iterator {
	return nil
}

func (table *FaultyTable) Size() (int, error) {
	return 0, table.SizeErr
}
