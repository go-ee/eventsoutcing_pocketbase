package espocketbase

import (
	"github.com/pocketbase/pocketbase/models/schema"
)

const UsersCollName = "users"
const UsersFieldUsername = "username"
const UsersFieldName = "name"
const UsersFieldEmail = "email"
const UsersFieldAvatar = "avatar"

func NewUsersDb() *Users {
	return &Users{
		CollectionBase: &CollectionBase{},
	}
}

type Users struct {
	*CollectionBase
}

func (db *Users) CheckOrCreateCollection() (err error) {
	if db.coll == nil {
		if db.coll, err = db.Dao.FindCollectionByNameOrId(UsersCollName); err != nil {
			return
		}
	}
	if _, ok := db.coll.Schema.AsMap()[FieldAdmin]; !ok {
		db.coll.Schema.AddField(&schema.SchemaField{
			Name: FieldAdmin,
			Type: schema.FieldTypeBool,
		})
		err = db.Dao.SaveCollection(db.coll)
	}
	return
}

type User struct {
	Email    string `json:"email,omitempty"`
	Name     string `json:"name,omitempty"`
	Username string `json:"username,omitempty"`
	Admin    bool   `json:"admin,omitempty"`
}
