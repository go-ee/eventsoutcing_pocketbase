package snapshotstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	es "github.com/go-ee/eventsoutcing_pocketbase"
	"github.com/go-ee/eventsoutcing_pocketbase/eventstore"
	"github.com/hallgren/eventsourcing/core"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/daos"
	"github.com/pocketbase/pocketbase/models"
	"github.com/pocketbase/pocketbase/models/schema"
	"github.com/pocketbase/pocketbase/tools/types"
)

const AggTypeFieldAggId = "agg_id"
const AggTypeFieldVersion = "version"
const AggTypeFieldGlobalVersion = "global_version"
const AggTypeFieldState = "state"

func New(store *eventstore.StoreCollections) *StoreCollections {
	return &StoreCollections{
		ColBase:    &es.ColBase{Env: store.Env},
		EventStore: store,

		aggTypeCols: map[string]*SnapCol{},
	}
}

type StoreCollections struct {
	*es.ColBase
	EventStore *eventstore.StoreCollections

	aggTypeCols map[string]*SnapCol
	auth        *es.AuthorizationBuilder
}

func (o *StoreCollections) Get(
	ctx context.Context, aggId string, aggregateType string) (ret core.Snapshot, err error) {

	var aggregateTypeCollection *SnapCol
	if aggregateTypeCollection, err = o.GetOrCreateForAggType(aggregateType); err != nil {
		return
	}

	ret, err = aggregateTypeCollection.Get(ctx, aggId, aggregateType)
	return
}

// Save persists events to the collections for aggregate type
func (o *StoreCollections) Save(snapshot core.Snapshot) (err error) {
	var aggTypeCol *SnapCol
	if aggTypeCol, err = o.GetOrCreateForAggType(snapshot.Type); err != nil {
		return
	}

	err = aggTypeCol.Save(snapshot)
	return
}

func (o *StoreCollections) GetOrCreateForAggType(aggType string) (ret *SnapCol, err error) {
	ret = o.aggTypeCols[aggType]
	if ret == nil {
		var eventStore *eventstore.AggCol
		if eventStore, err = o.EventStore.GetOrCreateForAggType(aggType); err != nil {
			return
		}
		ret = NewSnapCol(aggType, eventStore.Auth.AuthBuilder, o.Env)
		o.aggTypeCols[aggType] = ret
	}
	_, err = ret.CheckOrInit()
	return
}

func buildAggregateTypeCollectionName(aggregationType string) (ret string) {
	return fmt.Sprintf("%v_snap", es.ToSnakeCase(aggregationType))
}

func NewSnapshot(record *models.Record, aggregateType string) (ret *core.Snapshot) {
	ret = &core.Snapshot{
		ID:            record.GetString(AggTypeFieldAggId),
		Type:          aggregateType,
		Version:       core.Version(record.GetInt(AggTypeFieldVersion)),
		GlobalVersion: core.Version(record.GetInt(AggTypeFieldGlobalVersion)),
		State:         record.Get(AggTypeFieldState).(types.JsonRaw),
	}
	return
}

func NewRecord(snapshot *core.Snapshot, coll *models.Collection) (ret *models.Record) {
	ret = models.NewRecord(coll)

	ret.Set(AggTypeFieldAggId, snapshot.ID)
	ret.Set(AggTypeFieldVersion, uint64(snapshot.Version))
	ret.Set(AggTypeFieldGlobalVersion, uint64(snapshot.GlobalVersion))
	ret.Set(AggTypeFieldState, snapshot.State)
	return
}

func NewSnapCol(aggregateType string, auth *es.AuthorizationBuilder, env es.Env) *SnapCol {
	collectionName := buildAggregateTypeCollectionName(aggregateType)
	return &SnapCol{
		ColBase:        &es.ColBase{Env: env},
		auth:           auth,
		CollectionName: collectionName,
		AggregateType:  aggregateType,
	}
}

type SnapCol struct {
	*es.ColBase
	auth           *es.AuthorizationBuilder
	CollectionName string
	AggregateType  string
}

func (o *SnapCol) CheckOrInit() (ret bool, err error) {
	if ret, err = o.ColBase.CheckOrInit(); ret || err != nil {
		return
	}

	if o.Coll, err = o.Dao().FindCollectionByNameOrId(o.CollectionName); o.Coll == nil || o.IsRecreateDb() {
		if o.Coll != nil {
			if err = o.Dao().DeleteCollection(o.Coll); err != nil {
				return
			}
		}

		o.Coll = &models.Collection{
			Name: o.CollectionName,
			Type: models.CollectionTypeBase,
			Schema: schema.NewSchema(
				&schema.SchemaField{
					Name:     AggTypeFieldAggId,
					Type:     schema.FieldTypeText,
					Required: true,
				},
				&schema.SchemaField{
					Name:     AggTypeFieldVersion,
					Type:     schema.FieldTypeNumber,
					Required: true,
				},
				&schema.SchemaField{
					Name:     AggTypeFieldGlobalVersion,
					Type:     schema.FieldTypeNumber,
					Required: true,
				},
				&schema.SchemaField{
					Name: AggTypeFieldState,
					Type: schema.FieldTypeJson,
					Options: schema.JsonOptions{
						MaxSize: 102400,
					},
				},
			),
			Indexes: types.JsonArray[string]{fmt.Sprintf("CREATE INDEX idx_%v ON %v (%v)",
				AggTypeFieldAggId, o.CollectionName, AggTypeFieldAggId),
			},
			ListRule:   types.Pointer(o.auth.ListRule()),
			ViewRule:   types.Pointer(o.auth.ViewRule()),
			CreateRule: types.Pointer(o.auth.CreateRule()),
			UpdateRule: types.Pointer(o.auth.UpdateRule()),
			DeleteRule: types.Pointer(o.auth.DeleteRule()),
		}

		err = o.Dao().SaveCollection(o.Coll)
	}
	return
}

func (o *SnapCol) Get(
	_ context.Context, aggId string, aggregateType string) (ret core.Snapshot, err error) {
	fmt.Printf("aggregateType: %v, %v: %v", aggregateType, AggTypeFieldAggId, aggId)
	var record *models.Record
	if record, err = o.Dao().FindFirstRecordByFilter(o.Coll.Id,
		fmt.Sprintf("%v = {:%v}", AggTypeFieldAggId, AggTypeFieldAggId),
		dbx.Params{AggTypeFieldAggId: aggId},
	); err != nil {
		//TODO maybe we need to ignore );sql.ErrNoRows {
		return
	}

	ret = core.Snapshot{
		ID:            aggId,
		Type:          aggregateType,
		Version:       core.Version(record.GetInt(AggTypeFieldVersion)),
		GlobalVersion: core.Version(record.GetInt(AggTypeFieldGlobalVersion)),
		State:         record.Get(AggTypeFieldState).(types.JsonRaw),
	}

	return
}

// Save persists events to the collections for aggregate type
func (o *SnapCol) Save(snapshot core.Snapshot) (err error) {
	err = o.Dao().RunInTransaction(func(txDao *daos.Dao) (txErr error) {
		fmt.Printf("aggregateType: %v, %v: %v", snapshot.Type, AggTypeFieldAggId, snapshot.ID)
		var record *models.Record
		if record, txErr = txDao.FindFirstRecordByFilter(o.Coll.Id,
			fmt.Sprintf("%v = {:%v}", AggTypeFieldAggId, AggTypeFieldAggId),
			dbx.Params{AggTypeFieldAggId: snapshot.ID},
		); txErr != nil && !errors.Is(txErr, sql.ErrNoRows) {
			return
		}

		if record == nil {
			record = models.NewRecord(o.Coll)
			record.Set(AggTypeFieldAggId, snapshot.ID)
		}
		record.Set(AggTypeFieldVersion, uint64(snapshot.Version))
		record.Set(AggTypeFieldGlobalVersion, uint64(snapshot.GlobalVersion))
		record.Set(AggTypeFieldState, snapshot.State)

		txErr = txDao.Save(record)
		return
	})
	return
}
