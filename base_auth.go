package espocketbase

import (
	"fmt"
	"github.com/pocketbase/pocketbase/models"
	"github.com/pocketbase/pocketbase/models/schema"
	"github.com/pocketbase/pocketbase/tools/types"
)

func NewCollectionBaseAuth(collectionName string, fieldKey string, users *Users, roles []string) *CollectionBaseAuth {
	return &CollectionBaseAuth{
		CollectionBase: &CollectionBase{},
		AuthBuilder:    NewAuthorizationBuilder(collectionName+"_auth", fieldKey, roles),
		Users:          users,
	}
}

type CollectionBaseAuth struct {
	*CollectionBase
	Users       *Users
	AuthBuilder *AuthorizationBuilder
}

func (db *CollectionBaseAuth) CheckOrCreateCollection() (err error) {
	if db.coll != nil && !db.RecreateDb {
		return
	}

	if db.coll, err = db.Dao.FindCollectionByNameOrId(db.AuthBuilder.CollectionName); db.coll == nil || db.RecreateDb {
		if db.coll != nil {
			if err = db.Dao.DeleteCollection(db.coll); err != nil {
				return
			}
		}

		db.coll = &models.Collection{
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
			db.coll.Schema.AddField(
				&schema.SchemaField{
					Name: db.AuthBuilder.AuthFieldFor(role),
					Type: schema.FieldTypeRelation,
					Options: &schema.RelationOptions{
						CollectionId:  db.Users.coll.Id,
						CascadeDelete: false,
					},
				})
		}

		err = db.Dao.SaveCollection(db.coll)
	}
	return
}
