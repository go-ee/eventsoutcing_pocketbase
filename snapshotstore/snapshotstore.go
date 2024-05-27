package snapshotstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	es "github.com/go-ee/eventsoutcing_pocketbase"
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

func NewAggregateCollections(users *es.Users, authRoles []string, env es.Env) *AggregateCollections {
	return &AggregateCollections{
		CollectionBase: &es.CollectionBase{Env: env},
		Users:          users,
		AuthRoles:      authRoles,

		aggTypeCollections: map[string]*AggregateCollection{},
	}
}

type AggregateCollections struct {
	*es.CollectionBase
	aggTypeCollections map[string]*AggregateCollection
	Users              *es.Users
	AuthRoles          []string
}

func (o *AggregateCollections) Get(
	ctx context.Context, aggId string, aggregateType string) (ret core.Snapshot, err error) {

	var aggregateTypeCollection *AggregateCollection
	if aggregateTypeCollection, err = o.GetOrCreateForAggregationType(aggregateType); err != nil {
		return
	}

	ret, err = aggregateTypeCollection.Get(ctx, aggId, aggregateType)
	return
}

// Save persists events to the collections for aggregate type
func (o *AggregateCollections) Save(snapshot core.Snapshot) (err error) {
	var aggregateTypeCollection *AggregateCollection
	if aggregateTypeCollection, err = o.GetOrCreateForAggregationType(snapshot.Type); err != nil {
		return
	}

	err = aggregateTypeCollection.Save(snapshot)
	return
}

func (o *AggregateCollections) GetOrCreateForAggregationType(aggregateType string) (ret *AggregateCollection, err error) {
	ret = o.aggTypeCollections[aggregateType]
	if ret == nil {
		ret = NewAggregateCollection(aggregateType, o.Users, o.AuthRoles, o.Env)
		o.aggTypeCollections[aggregateType] = ret
	}
	_, err = ret.CheckOrInit()
	return
}

func buildAggregateTypeCollectionName(aggregationType string) (ret string) {
	return fmt.Sprintf("%v_snapshot", es.ToSnakeCase(aggregationType))
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

func NewAggregateCollection(aggregateType string, users *es.Users, authRoles []string, env es.Env) *AggregateCollection {
	collectionName := buildAggregateTypeCollectionName(aggregateType)
	return &AggregateCollection{
		CollectionBase: &es.CollectionBase{Env: env},
		auth:           es.NewCollectionBaseAuth(collectionName, AggTypeFieldAggId, users, authRoles, env),
		CollectionName: collectionName,
		AggregateType:  aggregateType,
	}
}

type AggregateCollection struct {
	*es.CollectionBase
	auth           *es.CollectionBaseAuth
	CollectionName string
	AggregateType  string
}

func (o *AggregateCollection) CheckOrInit() (ret bool, err error) {
	if ret, err = o.auth.CheckOrInit(); err != nil {
		return
	}

	if ret, err = o.CollectionBase.CheckOrInit(); ret || err != nil {
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
			ListRule:   types.Pointer(o.auth.AuthBuilder.ListRule()),
			ViewRule:   types.Pointer(o.auth.AuthBuilder.ViewRule()),
			CreateRule: types.Pointer(o.auth.AuthBuilder.CreateRule()),
			UpdateRule: types.Pointer(o.auth.AuthBuilder.UpdateRule()),
			DeleteRule: types.Pointer(o.auth.AuthBuilder.DeleteRule()),
		}

		err = o.Dao().SaveCollection(o.Coll)
	}
	return
}

func (o *AggregateCollection) Get(
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
func (o *AggregateCollection) Save(snapshot core.Snapshot) (err error) {
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
