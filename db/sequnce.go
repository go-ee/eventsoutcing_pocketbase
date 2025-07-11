package db

import (
	"database/sql"
	"fmt"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/types"
	"time"
)

const (
	SequenceColName   = "sequence"
	FieldName         = "name"
	FieldCurrentValue = "current_value"
	FieldLastUpdated  = "last_updated"
)

func NewSequence(env Env) *Sequence {
	return &Sequence{
		CollectionBase: CollectionBase{Name: SequenceColName, Env: env},
	}
}

type Sequence struct {
	CollectionBase
}

func (s *Sequence) Load() (err error) {
	if s.Collection != nil && !s.IsRecreateDb() {
		return
	}

	dao := s.App()
	if s.Collection, err = dao.FindCollectionByNameOrId(s.Name); s.Collection == nil || s.IsRecreateDb() {
		if s.Collection != nil {
			if err = dao.Delete(s.Collection); err != nil {
				return
			}
		}

		s.Collection = core.NewBaseCollection(s.Name)
		s.Collection.Fields.Add(
			&core.TextField{
				Name:     FieldName,
				Required: true,
			},
			&core.NumberField{
				Name:     FieldCurrentValue,
				Required: true,
				Min:      types.Pointer(0.0),
			},
			&core.DateField{
				Name:     FieldLastUpdated,
				Required: true,
			},
		)

		indexName := fmt.Sprintf("idx_%v_%v", s.Name, FieldName)
		s.Collection.AddIndex(indexName, true, FieldName, "")

		err = dao.Save(s.Collection)
	}
	return
}

// Helper to find or create a sequence record
func (s *Sequence) findOrCreateSequence(txApp core.App, sequenceName string, initialValue int) (
	record *core.Record, currentValue int, err error) {

	if record, err = txApp.FindFirstRecordByFilter(
		s.Name, "name = {:name}", dbx.Params{"name": sequenceName},
	); err != nil {
		if err == sql.ErrNoRows {
			record = core.NewRecord(s.Collection)
			record.Set(FieldName, sequenceName)
			record.Set(FieldCurrentValue, initialValue)
			record.Set(FieldLastUpdated, time.Now())

			if err = txApp.Save(record); err != nil {
				return
			}

			currentValue = initialValue
			err = nil

			return
		}
		return
	}
	currentValue = record.GetInt(FieldCurrentValue)
	return
}

func (s *Sequence) GetNext(sequenceName string) (ret int, err error) {
	err = s.App().RunInTransaction(func(txApp core.App) (txErr error) {
		ret, txErr = s.GetNextTx(txApp, sequenceName)
		return
	})
	return
}

func (s *Sequence) GetNextMultiple(sequenceName string, count int) (ret []int, err error) {
	err = s.App().RunInTransaction(func(txApp core.App) (txErr error) {
		ret, txErr = s.GetNextMultipleTx(txApp, sequenceName, count)
		return
	})
	return
}

func (s *Sequence) GetNextTx(txApp core.App, sequenceName string) (ret int, err error) {
	var sequence *core.Record
	var currentVal int
	if sequence, currentVal, err = s.findOrCreateSequence(txApp, sequenceName, 1); err != nil {
		return
	}

	if currentVal == 1 && sequence.GetInt(FieldCurrentValue) == 1 {
		ret = 1
	} else {
		ret = currentVal + 1
		sequence.Set(FieldCurrentValue, ret)
		sequence.Set(FieldLastUpdated, time.Now())
		err = txApp.Save(sequence)
	}
	return
}

func (s *Sequence) GetNextMultipleTx(txApp core.App, sequenceName string, count int) (ret []int, err error) {
	if count <= 0 {
		err = fmt.Errorf("count must be positive")
		return
	}

	var sequence *core.Record
	var currentVal int
	if sequence, currentVal, err = s.findOrCreateSequence(txApp, sequenceName, count); err != nil {
		return
	}

	var startVal int
	if currentVal == count && sequence.GetInt(FieldCurrentValue) == count {
		startVal = 1
	} else {
		startVal = currentVal + 1
		sequence.Set(FieldCurrentValue, currentVal+count)
		sequence.Set(FieldLastUpdated, time.Now())
		if err = txApp.Save(sequence); err != nil {
			return
		}
	}

	ret = make([]int, count)
	for i := 0; i < count; i++ {
		ret[i] = startVal + i
	}
	return
}
