package espocketbase

import (
	"github.com/pocketbase/pocketbase/daos"
	"github.com/pocketbase/pocketbase/models"
)

type ColBase struct {
	Env
	Coll *models.Collection
}

func (db *ColBase) CheckOrInit() (ret bool, err error) {
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
