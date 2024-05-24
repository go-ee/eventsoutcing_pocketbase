package espocketbase

import (
	"github.com/pocketbase/pocketbase/models/schema"
)

const UsersCollName = "users"
const UsersFieldUsername = "username"
const UsersFieldName = "name"
const UsersFieldEmail = "email"
const UsersFieldAvatar = "avatar"

func NewUsersDb(env Env) *Users {
	return &Users{
		CollectionBase: &CollectionBase{Env: env},
	}
}

type Users struct {
	*CollectionBase
}

func (db *Users) CheckOrInit() (ret bool, err error) {
	if ret, err = db.CollectionBase.CheckOrInit(); ret || err != nil {
		return
	}
	if db.Coll, err = db.Dao().FindCollectionByNameOrId(UsersCollName); err != nil {
		return
	}

	if _, ok := db.Coll.Schema.AsMap()[FieldAdmin]; !ok {
		db.Coll.Schema.AddField(&schema.SchemaField{
			Name: FieldAdmin,
			Type: schema.FieldTypeBool,
		})
		err = db.Dao().SaveCollection(db.Coll)
	}
	return
}

type User struct {
	Email    string `json:"email,omitempty"`
	Name     string `json:"name,omitempty"`
	Username string `json:"username,omitempty"`
	Admin    bool   `json:"admin,omitempty"`
}
