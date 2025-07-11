package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	es "github.com/go-ee/eventsoutcing_pocketbase"
	"github.com/go-ee/eventsoutcing_pocketbase/db"
	"github.com/hallgren/eventsourcing/core"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/tools/types"

	pbcore "github.com/pocketbase/pocketbase/core"
)

const aggregates_golabl_version = "aggregates_global_version"

const AggTypeFieldAggId = "agg_id"
const AggTypeFieldVersion = "version"
const AggTypeFieldGlobalVersion = "global_version"
const AggTypeFieldReason = "reason"
const AggTypeFieldTimestamp = "timestamp"
const AggTypeFieldData = "data"
const AggTypeFieldMetadata = "metadata"

func New(user *db.User, authRoles []string, env db.Env) *Store {
	return &Store{
		CollectionBase: db.CollectionBase{Env: env},
		User:           user,
		Sequence:       db.NewSequence(env),
		AuthRoles:      authRoles,

		aggTypeCols: map[string]*Aggregate{},
	}
}

type Store struct {
	db.CollectionBase
	User      *db.User
	Sequence  *db.Sequence
	AuthRoles []string

	aggTypeCols map[string]*Aggregate
}

func (store *Store) Load() (err error) {
	err = store.Sequence.Load()
	return
}

func (store *Store) Get(
	ctx context.Context, aggregateId string, aggregateType string, afterVersion core.Version) (ret core.Iterator, err error) {

	var aggTypeCollection *Aggregate
	if aggTypeCollection, err = store.GetOrCreateForAggType(aggregateType); err != nil {
		return
	}

	ret, err = aggTypeCollection.Get(ctx, aggregateId, aggregateType, afterVersion)
	return
}

// Save persists events to the collections for aggregate type
func (store *Store) Save(events []core.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	//aggregateID := events[0].AggregateID
	aggType := events[0].AggregateType

	var aggTypeCollection *Aggregate
	if aggTypeCollection, err = store.GetOrCreateForAggType(aggType); err != nil {
		return
	}

	err = aggTypeCollection.Save(events)
	return
}

func (store *Store) GetOrCreateForAggType(aggType string) (ret *Aggregate, err error) {
	ret = store.aggTypeCols[aggType]
	if ret == nil {
		ret = NewAggregate(aggType, store.User, store.AuthRoles, store.Sequence, store.Env)
		store.aggTypeCols[aggType] = ret
	}
	err = ret.Load()
	return
}

func buildAggTypeColName(aggType string) (ret string) {
	return es.ToSnakeCase(aggType)
}

func NewEvent(record *pbcore.Record, aggType string) (ret *core.Event) {
	ret = &core.Event{
		AggregateID:   record.GetString(AggTypeFieldAggId),
		Version:       core.Version(record.GetInt(AggTypeFieldVersion)),
		GlobalVersion: core.Version(record.GetInt(AggTypeFieldGlobalVersion)),
		AggregateType: aggType,
		Reason:        record.GetString(AggTypeFieldReason),
		Timestamp:     record.GetDateTime(AggTypeFieldTimestamp).Time(),
		Data:          record.Get(AggTypeFieldData).(types.JSONRaw),
		Metadata:      record.Get(AggTypeFieldMetadata).(types.JSONRaw),
	}
	return
}

func NewRecord(event *core.Event, coll *pbcore.Collection) (ret *pbcore.Record) {
	ret = pbcore.NewRecord(coll)

	ret.Set(AggTypeFieldAggId, event.AggregateID)
	ret.Set(AggTypeFieldVersion, uint64(event.Version))
	ret.Set(AggTypeFieldGlobalVersion, uint64(event.GlobalVersion))
	ret.Set(AggTypeFieldReason, event.Reason)
	ret.Set(AggTypeFieldTimestamp, event.Timestamp)
	ret.Set(AggTypeFieldData, event.Data)
	ret.Set(AggTypeFieldMetadata, event.Metadata)
	return
}

func NewAggregate(aggType string, user *db.User, authRoles []string, sequence *db.Sequence, env db.Env) *Aggregate {
	colName := buildAggTypeColName(aggType)
	return &Aggregate{
		Auth:          db.NewAuth(colName, AggTypeFieldAggId, user, authRoles, env),
		Sequence:      sequence,
		AggregateType: aggType,
	}
}

type Aggregate struct {
	db.Auth
	Sequence      *db.Sequence
	AggregateType string
}

func (o *Aggregate) Load() (err error) {
	if !o.IsAuthDisabled() {
		if err = o.Auth.Load(); err != nil {
			return
		}
	}

	if o.Collection != nil && !o.IsRecreateDb() {
		return
	}

	dao := o.App()
	if o.Collection, err = dao.FindCollectionByNameOrId(o.Name); o.Collection == nil || o.IsRecreateDb() {
		if o.Collection != nil {
			if err = dao.Delete(o.Collection); err != nil {
				return
			}
		}

		o.Collection = pbcore.NewBaseCollection(o.Name)
		o.Collection.Fields.Add(
			&pbcore.TextField{
				Name:     AggTypeFieldAggId,
				Required: true,
			},
			&pbcore.NumberField{
				Name:     AggTypeFieldVersion,
				Required: true,
			},
			&pbcore.NumberField{
				Name:     AggTypeFieldGlobalVersion,
				Required: true,
			},
			&pbcore.TextField{
				Name:     AggTypeFieldReason,
				Required: true,
			},
			&pbcore.DateField{
				Name:     AggTypeFieldTimestamp,
				Required: true,
			},
			&pbcore.JSONField{
				Name:     AggTypeFieldMetadata,
				Required: true,
				MaxSize:  102400,
			},
		)
		indexName := fmt.Sprintf("idx_%v_%v", o.Name, AggTypeFieldAggId)
		o.Collection.AddIndex(indexName, true, AggTypeFieldAggId, "")

		if !o.IsAuthDisabled() {
			o.Collection.ListRule = types.Pointer(o.Auth.AuthBuilder.ListRule())
			o.Collection.ViewRule = types.Pointer(o.Auth.AuthBuilder.ViewRule())
			o.Collection.CreateRule = types.Pointer(o.Auth.AuthBuilder.CreateRule())
			o.Collection.UpdateRule = types.Pointer(o.Auth.AuthBuilder.UpdateRule())
			o.Collection.DeleteRule = types.Pointer(o.Auth.AuthBuilder.DeleteRule())
		} else {
			db.DisableAuth(o.Collection)
		}

		err = dao.Save(o.Collection)
	}
	return
}

func (o *Aggregate) Get(_ context.Context,
	aggId string, aggType string, afterVersion core.Version) (ret core.Iterator, err error) {

	fmt.Printf("aggType: %v, %v: %v", aggType, AggTypeFieldAggId, aggId)
	var records []*pbcore.Record
	dao := o.App()
	if records, err = dao.FindRecordsByFilter(o.Collection.Id,
		fmt.Sprintf("%v = {:%v} and %v > {:%v}", AggTypeFieldAggId, AggTypeFieldAggId,
			AggTypeFieldVersion, AggTypeFieldVersion), AggTypeFieldVersion+" asc", 0, 0,
		dbx.Params{AggTypeFieldAggId: aggId, AggTypeFieldVersion: afterVersion},
	); err != nil {
		//TODO maybe we need to ignore );sql.ErrNoRows {
		return
	}
	ret = NewIterator(aggType, records)
	return
}

// Save persists events to the collections for aggregate type
func (o *Aggregate) Save(events []core.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	err = o.App().RunInTransaction(func(txApp pbcore.App) (txErr error) {
		aggId := events[0].AggregateID
		aggType := events[0].AggregateType
		fmt.Printf("aggType: %v, %v: %v", aggType, AggTypeFieldAggId, aggId)

		var record *pbcore.Record
		if record, txErr = txApp.FindFirstRecordByFilter(o.Collection.Id,
			fmt.Sprintf("%v = {:%v}", AggTypeFieldAggId, AggTypeFieldAggId),
			dbx.Params{AggTypeFieldAggId: aggId},
		); txErr != nil && !errors.Is(txErr, sql.ErrNoRows) {
			return
		}

		var currentVersion core.Version
		if record != nil {
			currentVersion = core.Version(record.GetInt(AggTypeFieldVersion))
		} else {
			currentVersion = core.Version(0)
		}

		// Make sure no other has saved event to the same aggregate concurrently
		firstEventVersion := events[0].Version
		if currentVersion+1 != firstEventVersion {
			txErr = core.ErrConcurrency
			return
		}

		var globalVersions []int
		if globalVersions, txErr = o.Sequence.GetNextMultipleTx(txApp, aggregates_golabl_version, len(events)); err != nil {
			return
		}

		for i, event := range events {

			event.GlobalVersion = core.Version(globalVersions[i])

			record = NewRecord(&event, o.Collection)

			if txErr = txApp.Save(record); txErr != nil {
				return
			}
		}
		return
	})
	return
}
