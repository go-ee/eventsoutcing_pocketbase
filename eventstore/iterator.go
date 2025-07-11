package eventstore

import (
	"github.com/hallgren/eventsourcing/core"
	pbcore "github.com/pocketbase/pocketbase/core"
)

func NewIterator(aggregateType string, records []*pbcore.Record) *Iterator {
	return &Iterator{
		aggregateType: aggregateType,
		records:       records,
		recordsCount:  len(records),
	}
}

type Iterator struct {
	aggregateType string
	records       []*pbcore.Record
	currentIndex  int
	recordsCount  int
}

// Next return true if there are more data
func (i *Iterator) Next() bool {
	i.currentIndex++
	return i.currentIndex < i.recordsCount
}

// Value return the event
func (i *Iterator) Value() (ret core.Event, err error) {
	currentRecord := i.records[i.currentIndex]
	ret = *NewEvent(currentRecord, i.aggregateType)
	return
}

// Close closes the iterator
func (i *Iterator) Close() {
	i.records = nil
}
