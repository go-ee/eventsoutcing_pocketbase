package db

import "github.com/pocketbase/pocketbase/core"

type CollectionBase struct {
	Env
	*core.Collection

	Name string
}

func (col *CollectionBase) CheckOrInit() (ret bool, err error) {
	ret = col.Collection != nil
	return
}

type Collection interface {
	Load() error
}

type Env interface {
	App() core.App
	IsRecreateDb() bool
	IsRecreateDbAuth() bool
	IsAuthDisabled() bool
}
