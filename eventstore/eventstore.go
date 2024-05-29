package eventstore

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
const AggTypeFieldReason = "reason"
const AggTypeFieldTimestamp = "timestamp"
const AggTypeFieldData = "data"
const AggTypeFieldMetadata = "metadata"

func New(usersColId string, authRoles []string, env es.Env) *StoreCollections {
	return &StoreCollections{
		ColBase:    &es.ColBase{Env: env},
		UsersColId: usersColId,
		AuthRoles:  authRoles,

		aggTypeCols: map[string]*AggCol{},
	}
}

type StoreCollections struct {
	*es.ColBase
	UsersColId string
	AuthRoles  []string

	aggTypeCols map[string]*AggCol
}

func (o *StoreCollections) Get(
	ctx context.Context, aggregateId string, aggregateType string, afterVersion core.Version) (ret core.Iterator, err error) {

	var aggTypeCollection *AggCol
	if aggTypeCollection, err = o.GetOrCreateForAggType(aggregateType); err != nil {
		return
	}

	ret, err = aggTypeCollection.Get(ctx, aggregateId, aggregateType, afterVersion)
	return
}

// Save persists events to the collections for aggregate type
func (o *StoreCollections) Save(events []core.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	//aggregateID := events[0].AggregateID
	aggType := events[0].AggregateType

	var aggTypeCollection *AggCol
	if aggTypeCollection, err = o.GetOrCreateForAggType(aggType); err != nil {
		return
	}

	err = aggTypeCollection.Save(events)
	return
}

func (o *StoreCollections) GetOrCreateForAggType(aggType string) (ret *AggCol, err error) {
	ret = o.aggTypeCols[aggType]
	if ret == nil {
		ret = NewAggCol(aggType, o.UsersColId, o.AuthRoles, o.Env)
		o.aggTypeCols[aggType] = ret
	}
	_, err = ret.CheckOrInit()
	return
}

func buildAggTypeColName(aggType string) (ret string) {
	return es.ToSnakeCase(aggType)
}

func NewEvent(record *models.Record, aggType string) (ret *core.Event) {
	ret = &core.Event{
		AggregateID:   record.GetString(AggTypeFieldAggId),
		Version:       core.Version(record.GetInt(AggTypeFieldVersion)),
		GlobalVersion: core.Version(record.GetInt(AggTypeFieldGlobalVersion)),
		AggregateType: aggType,
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

func NewAggCol(aggType string, usersColId string, authRoles []string, env es.Env) *AggCol {
	colName := buildAggTypeColName(aggType)
	return &AggCol{
		ColBase:       &es.ColBase{Env: env},
		Auth:          es.NewCollectionBaseAuth(colName, AggTypeFieldAggId, usersColId, authRoles, env),
		ColName:       colName,
		AggregateType: aggType,
	}
}

type AggCol struct {
	*es.ColBase
	Auth          *es.ColBaseAuth
	ColName       string
	AggregateType string
}

func (o *AggCol) CheckOrInit() (ret bool, err error) {
	if ret, err = o.Auth.CheckOrInit(); err != nil {
		return
	}

	if ret, err = o.ColBase.CheckOrInit(); ret || err != nil {
		return
	}

	if o.Coll, err = o.Dao().FindCollectionByNameOrId(o.ColName); o.Coll == nil || o.IsRecreateDb() {
		if o.Coll != nil {
			if err = o.Dao().DeleteCollection(o.Coll); err != nil {
				return
			}
		}

		o.Coll = &models.Collection{
			Name: o.ColName,
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
				fmt.Sprintf("CREATE INDEX idx_%v ON %v (%v)", AggTypeFieldAggId, o.ColName, AggTypeFieldAggId),
			},
			ListRule:   types.Pointer(o.Auth.AuthBuilder.ListRule()),
			ViewRule:   types.Pointer(o.Auth.AuthBuilder.ViewRule()),
			CreateRule: types.Pointer(o.Auth.AuthBuilder.CreateRule()),
			UpdateRule: types.Pointer(o.Auth.AuthBuilder.UpdateRule()),
			DeleteRule: types.Pointer(o.Auth.AuthBuilder.DeleteRule()),
		}

		err = o.Dao().SaveCollection(o.Coll)
	}
	return
}

func (o *AggCol) Get(_ context.Context,
	aggId string, aggType string, afterVersion core.Version) (ret core.Iterator, err error) {

	fmt.Printf("aggType: %v, %v: %v", aggType, AggTypeFieldAggId, aggId)
	var records []*models.Record
	if records, err = o.Dao().FindRecordsByFilter(o.Coll.Id,
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
func (o *AggCol) Save(events []core.Event) (err error) {
	// If no event return no error
	if len(events) == 0 {
		return
	}

	err = o.Dao().RunInTransaction(func(txDao *daos.Dao) (txErr error) {
		aggId := events[0].AggregateID
		aggType := events[0].AggregateType
		fmt.Printf("aggType: %v, %v: %v", aggType, AggTypeFieldAggId, aggId)
		var record *models.Record
		if record, txErr = txDao.FindFirstRecordByFilter(o.Coll.Id,
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
