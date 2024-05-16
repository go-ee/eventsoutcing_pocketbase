package espocketbase

import (
	"context"
	"fmt"
	"github.com/hallgren/eventsourcing"
	"github.com/hallgren/eventsourcing/core"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/daos"
	"github.com/pocketbase/pocketbase/models"
	"github.com/pocketbase/pocketbase/models/schema"
	"github.com/pocketbase/pocketbase/tools/types"
)

const AggTypesColName = "agg_types"
const AggTypesFieldAggId = "agg_type"
const AggTypesFieldAggType = "agg_id"

const aggTypeColNamePrefix = "es_"

const AggTypeFieldAggId = "agg_id"
const AggTypeFieldSeq = "seq"
const AggTypeFieldVersion = "version"
const AggTypeFieldGlobalVersion = "global_version"
const AggTypeFieldReason = "reason"
const AggTypeFieldTimestamp = "timestamp"
const AggTypeFieldData = "data"
const AggTypeFieldMetadata = "metadata"

func NewAggregateCollection(aggregateType string, users *Users) *AggregateCollection {
	collectionName := buildAggregateTypeCollectionName(aggregateType)
	return &AggregateCollection{
		CollectionBase: &CollectionBase{},
		auth: NewCollectionBaseAuth(collectionName, "key", users,
			[]string{"admin", "maintainer", "user"}),
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

func (o *AggregateCollection) CheckOrCreateCollection() (err error) {
	if err = o.auth.CheckOrCreateCollection(); err != nil {
		return
	}

	if o.coll != nil && !o.RecreateDb {
		return
	}

	if o.coll, err = o.Dao.FindCollectionByNameOrId(o.CollectionName); o.coll == nil || o.RecreateDb {
		if o.coll != nil {
			if err = o.Dao.DeleteCollection(o.coll); err != nil {
				return
			}
		}

		o.coll = &models.Collection{
			Name: o.CollectionName,
			Type: models.CollectionTypeBase,
			Schema: schema.NewSchema(
				&schema.SchemaField{
					Name:     AggTypeFieldAggId,
					Type:     schema.FieldTypeNumber,
					Required: true,
				},
				&schema.SchemaField{
					Name:     AggTypeFieldSeq,
					Type:     schema.FieldTypeNumber,
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
			ListRule:   types.Pointer(o.auth.AuthBuilder.ListRule()),
			ViewRule:   types.Pointer(o.auth.AuthBuilder.ViewRule()),
			CreateRule: types.Pointer(o.auth.AuthBuilder.CreateRule()),
			UpdateRule: types.Pointer(o.auth.AuthBuilder.UpdateRule()),
			DeleteRule: types.Pointer(o.auth.AuthBuilder.DeleteRule()),
		}

		err = o.Dao.SaveCollection(o.coll)
	}
	return
}

func (o *AggregateCollection) Get(ctx context.Context,
	aggregateId string, aggregateType string, afterVersion core.Version) (ret core.Iterator, err error) {

	fmt.Printf("aggregateType: %v, aggregateID: %v", aggregateType, aggregateId)
	var records []*models.Record
	records, err = o.Dao.FindRecordsByFilter(o.coll.Id,
		"aggregate_id = {:aggregate_id} and version > {:version}", "version asc", 0, 0,
		dbx.Params{"aggregate_id": aggregateId, "version": afterVersion},
	)
	ret = NewIterator(aggregateType, records)
	return
}

// Save persists events to the collections for aggregate type
func (o *AggregateCollection) Save(events []eventsourcing.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	err = o.Dao.RunInTransaction(func(txDao *daos.Dao) (txErr error) {
		aggregateID := events[0].AggregateID()
		aggregateType := events[0].AggregateType()
		fmt.Printf("aggregateType: %v, aggregateID: %v", aggregateType, aggregateID)
		var record *models.Record
		if record, txErr = txDao.FindFirstRecordByFilter(o.coll.Id, "aggregate_id = {:aggregate_id}",
			dbx.Params{"aggregate_id": aggregateID},
		); txErr != nil {
			return
		}

		var currentVersion core.Version
		if record != nil {
			currentVersion = core.Version(record.GetInt(AggTypeFieldVersion))
		} else {
			currentVersion = core.Version(0)
		}

		// Make sure no other has saved event to the same aggregate concurrently
		firstEventVersion := events[0].Version()
		if currentVersion+1 != core.Version(firstEventVersion) {
			txErr = core.ErrConcurrency
			return
		}

		for _, event := range events {
			record = NewRecord(&event, o.coll)

			if txErr = txDao.Save(record); txErr != nil {
				return
			}
		}
		return
	})
	return
}

func NewAggregateCollections(users *Users) *AggregateCollections {
	return &AggregateCollections{
		CollectionBase: &CollectionBase{},
		auth: NewCollectionBaseAuth(AggTypesColName, "agg", users,
			[]string{"admin", "maintainer", "user"}),
	}
}

type AggregateCollections struct {
	*CollectionBase
	auth               *CollectionBaseAuth
	aggTypeCollections map[string]*AggregateCollection
	users              *Users
}

func (o *AggregateCollections) Get(
	ctx context.Context, aggregateId string, aggregateType string, afterVersion core.Version) (ret core.Iterator, err error) {

	var aggregateTypeCollection *AggregateCollection
	if aggregateTypeCollection, err = o.CheckOrCreateCollection(aggregateType); err != nil {
		return
	}

	ret, err = aggregateTypeCollection.Get(ctx, aggregateId, aggregateType, afterVersion)
	return
}

// Save persists events to the collections for aggregate type
func (o *AggregateCollections) Save(events []eventsourcing.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	//aggregateID := events[0].AggregateID
	aggregateType := events[0].AggregateType()

	var aggregateTypeCollection *AggregateCollection
	if aggregateTypeCollection, err = o.CheckOrCreateCollection(aggregateType); err != nil {
		return
	}

	err = aggregateTypeCollection.Save(events)
	return
}

func (o *AggregateCollections) CheckOrCreateCollection(aggregateType string) (ret *AggregateCollection, err error) {
	ret = o.aggTypeCollections[aggregateType]
	if ret == nil {
		ret = NewAggregateCollection(aggregateType, o.users)
	}
	err = ret.CheckOrCreateCollection()
	return
}

type Collection interface {
	SetDaoAndRecreateDb(dao *daos.Dao, recreateDb bool)
	CheckOrCreateCollection() error
}

type CollectionBase struct {
	Dao        *daos.Dao
	RecreateDb bool

	coll *models.Collection
}

func (db *CollectionBase) SetDaoAndRecreateDb(dao *daos.Dao, recreateDb bool) {
	db.Dao = dao
	db.RecreateDb = recreateDb
}

func buildAggregateTypeCollectionName(aggregationType string) (ret string) {
	return fmt.Sprintf("%v%v", aggTypeColNamePrefix, aggregationType)
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

func NewRecord(event *eventsourcing.Event, coll *models.Collection) (ret *models.Record) {
	ret = models.NewRecord(coll)

	ret.Set(AggTypeFieldAggId, event.AggregateID)
	ret.Set(AggTypeFieldVersion, event.Version)
	ret.Set(AggTypeFieldGlobalVersion, event.GlobalVersion)
	ret.Set(AggTypeFieldReason, event.Reason)
	ret.Set(AggTypeFieldTimestamp, event.Timestamp)
	ret.Set(AggTypeFieldData, event.Data)
	ret.Set(AggTypeFieldMetadata, event.Metadata)
	return
}