package espocketbase

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/hallgren/eventsourcing/core"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/daos"
	"github.com/pocketbase/pocketbase/models"
	"github.com/pocketbase/pocketbase/models/schema"
	"github.com/pocketbase/pocketbase/tools/types"
)

const aggTypeColNamePrefix = "agg_"

const AggTypeFieldAggId = "agg_id"
const AggTypeFieldVersion = "version"
const AggTypeFieldGlobalVersion = "global_version"
const AggTypeFieldReason = "reason"
const AggTypeFieldTimestamp = "timestamp"
const AggTypeFieldData = "data"
const AggTypeFieldMetadata = "metadata"

func NewAggregateCollections(users *Users, authRoles []string, env Env) *AggregateCollections {
	return &AggregateCollections{
		CollectionBase: &CollectionBase{Env: env},
		Users:          users,
		AuthRoles:      authRoles,

		aggTypeCollections: map[string]*AggregateCollection{},
	}
}

type AggregateCollections struct {
	*CollectionBase
	aggTypeCollections map[string]*AggregateCollection
	Users              *Users
	AuthRoles          []string
}

func (o *AggregateCollections) Get(
	ctx context.Context, aggregateId string, aggregateType string, afterVersion core.Version) (ret core.Iterator, err error) {

	var aggregateTypeCollection *AggregateCollection
	if aggregateTypeCollection, err = o.GetOrCreateForAggregationType(aggregateType); err != nil {
		return
	}

	ret, err = aggregateTypeCollection.Get(ctx, aggregateId, aggregateType, afterVersion)
	return
}

// Save persists events to the collections for aggregate type
func (o *AggregateCollections) Save(events []core.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	//aggregateID := events[0].AggregateID
	aggregateType := events[0].AggregateType

	var aggregateTypeCollection *AggregateCollection
	if aggregateTypeCollection, err = o.GetOrCreateForAggregationType(aggregateType); err != nil {
		return
	}

	err = aggregateTypeCollection.Save(events)
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

type CollectionBase struct {
	Env
	Coll *models.Collection
}

func (db *CollectionBase) CheckOrInit() (ret bool, err error) {
	ret = db.Coll != nil
	return
}

type Collection interface {
	CheckOrInit() (bool, error)
}

type Env interface {
	Dao() *daos.Dao
	IsRecreateDb() bool
	IsAuthDisabled() bool
}

func buildAggregateTypeCollectionName(aggregationType string) (ret string) {
	return fmt.Sprintf("%v%v", aggTypeColNamePrefix, ToSnakeCase(aggregationType))
}

func NewEvent(record *models.Record, aggregateType string) (ret *core.Event) {
	ret = &core.Event{
		AggregateID:   record.GetString(AggTypeFieldAggId),
		Version:       core.Version(record.GetInt(AggTypeFieldVersion)),
		GlobalVersion: core.Version(record.GetInt(AggTypeFieldGlobalVersion)),
		AggregateType: aggregateType,
		Reason:        record.GetString(AggTypeFieldReason),
		Timestamp:     record.GetTime(AggTypeFieldTimestamp),
		Data:          record.Get(AggTypeFieldData).(types.JsonRaw),
		Metadata:      record.Get(AggTypeFieldMetadata).(types.JsonRaw),
	}
	return
}

func NewRecord(event *core.Event, coll *models.Collection) (ret *models.Record) {
	ret = models.NewRecord(coll)

	ret.Set(AggTypeFieldAggId, event.AggregateID)
	ret.Set(AggTypeFieldVersion, uint64(event.Version))
	ret.Set(AggTypeFieldGlobalVersion, uint64(event.GlobalVersion))
	ret.Set(AggTypeFieldReason, event.Reason)
	ret.Set(AggTypeFieldTimestamp, event.Timestamp)
	ret.Set(AggTypeFieldData, event.Data)
	ret.Set(AggTypeFieldMetadata, event.Metadata)
	return
}

func NewAggregateCollection(aggregateType string, users *Users, authRoles []string, env Env) *AggregateCollection {
	collectionName := buildAggregateTypeCollectionName(aggregateType)
	return &AggregateCollection{
		CollectionBase: &CollectionBase{Env: env},
		auth:           NewCollectionBaseAuth(collectionName, AggTypeFieldAggId, users, authRoles, env),
		CollectionName: collectionName,
		AggregateType:  aggregateType,
	}
}

type AggregateCollection struct {
	*CollectionBase
	auth           *CollectionBaseAuth
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
					Name:     AggTypeFieldReason,
					Type:     schema.FieldTypeText,
					Required: true,
				},
				&schema.SchemaField{
					Name:     AggTypeFieldTimestamp,
					Type:     schema.FieldTypeDate,
					Required: true,
				},
				&schema.SchemaField{
					Name: AggTypeFieldData,
					Type: schema.FieldTypeJson,
					Options: schema.JsonOptions{
						MaxSize: 102400,
					},
				},
				&schema.SchemaField{
					Name: AggTypeFieldMetadata,
					Type: schema.FieldTypeJson,
					Options: schema.JsonOptions{
						MaxSize: 102400,
					},
				},
			),
			Indexes: types.JsonArray[string]{
				fmt.Sprintf("CREATE INDEX idx_%v ON %v (%v)", AggTypeFieldAggId, o.CollectionName, AggTypeFieldAggId),
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

func (o *AggregateCollection) Get(ctx context.Context,
	aggrId string, aggregateType string, afterVersion core.Version) (ret core.Iterator, err error) {

	fmt.Printf("aggregateType: %v, %v: %v", aggregateType, AggTypeFieldAggId, aggrId)
	var records []*models.Record
	if records, err = o.Dao().FindRecordsByFilter(o.Coll.Id,
		fmt.Sprintf("%v = {:%v} and %v > {:%v}", AggTypeFieldAggId, AggTypeFieldAggId,
			AggTypeFieldVersion, AggTypeFieldVersion), AggTypeFieldVersion+" asc", 0, 0,
		dbx.Params{AggTypeFieldAggId: aggrId, AggTypeFieldVersion: afterVersion},
	); err != nil {
		//TODO maybe we need to ignore );sql.ErrNoRows {
		return
	}
	ret = NewIterator(aggregateType, records)
	return
}

// Save persists events to the collections for aggregate type
func (o *AggregateCollection) Save(events []core.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	err = o.Dao().RunInTransaction(func(txDao *daos.Dao) (txErr error) {
		aggrId := events[0].AggregateID
		aggregateType := events[0].AggregateType
		fmt.Printf("aggregateType: %v, %v: %v", aggregateType, AggTypeFieldAggId, aggrId)
		var record *models.Record
		if record, txErr = txDao.FindFirstRecordByFilter(o.Coll.Id,
			fmt.Sprintf("%v = {:%v}", AggTypeFieldAggId, AggTypeFieldAggId),
			dbx.Params{AggTypeFieldAggId: aggrId},
		); txErr != nil && txErr != sql.ErrNoRows {
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
		if currentVersion+1 != core.Version(firstEventVersion) {
			txErr = core.ErrConcurrency
			return
		}

		for _, event := range events {
			record = NewRecord(&event, o.Coll)

			if txErr = txDao.Save(record); txErr != nil {
				return
			}
		}
		return
	})
	return
}
