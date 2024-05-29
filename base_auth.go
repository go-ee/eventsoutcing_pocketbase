package espocketbase

import (
	"fmt"
	"github.com/pocketbase/pocketbase/models"
	"github.com/pocketbase/pocketbase/models/schema"
	"github.com/pocketbase/pocketbase/tools/types"
)

func NewCollectionBaseAuth(
	collectionName string, fieldKey string, usersColId string, roles []string, env Env) *ColBaseAuth {

	return &ColBaseAuth{
		ColBase:     &ColBase{Env: env},
		AuthBuilder: NewAuthorizationBuilder(collectionName+"_auth", fieldKey, roles),
		UsersColId:  usersColId,
	}
}

type ColBaseAuth struct {
	*ColBase
	UsersColId  string
	AuthBuilder *AuthorizationBuilder
}

func (db *ColBaseAuth) CheckOrInit() (ret bool, err error) {
	if ret, err = db.ColBase.CheckOrInit(); ret {
		return
	}

	if db.Coll, err = db.Dao().FindCollectionByNameOrId(db.AuthBuilder.CollectionName); db.Coll == nil || db.IsRecreateDb() {
		if db.Coll != nil {
			if err = db.Dao().DeleteCollection(db.Coll); err != nil {
				return
			}
		}

		db.Coll = &models.Collection{
			Name: db.AuthBuilder.CollectionName,
			Type: models.CollectionTypeBase,
			Schema: schema.NewSchema(
				&schema.SchemaField{
					Name:     db.AuthBuilder.FieldKey,
					Type:     schema.FieldTypeText,
					Required: true,
					Options: &schema.TextOptions{
						Min:     types.Pointer(2),
						Pattern: "",
					},
				},
			),
			Indexes: types.JsonArray[string]{
				fmt.Sprintf("CREATE UNIQUE INDEX idx_%v ON %v (%v)",
					db.AuthBuilder.FieldKey, db.AuthBuilder.CollectionName, db.AuthBuilder.FieldKey),
			},
			ListRule:   types.Pointer(db.AuthBuilder.AuthLoggedIn),
			ViewRule:   types.Pointer(db.AuthBuilder.AuthLoggedIn),
			CreateRule: types.Pointer(db.AuthBuilder.AuthLoggedIn),
			UpdateRule: types.Pointer(db.AuthBuilder.AuthLoggedIn),
			DeleteRule: types.Pointer(db.AuthBuilder.AuthLoggedIn),
		}

		for _, role := range db.AuthBuilder.Roles {
			db.Coll.Schema.AddField(
				&schema.SchemaField{
					Name: db.AuthBuilder.AuthFieldFor(role),
					Type: schema.FieldTypeRelation,
					Options: &schema.RelationOptions{
						CollectionId:  db.UsersColId,
						CascadeDelete: false,
					},
				})
		}

		err = db.Dao().SaveCollection(db.Coll)
	}
	return
}
