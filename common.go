package espocketbase

import (
	"github.com/pocketbase/pocketbase/daos"
	"github.com/pocketbase/pocketbase/models"
)

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
