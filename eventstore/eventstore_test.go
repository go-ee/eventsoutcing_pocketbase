package eventstore

import (
	"github.com/go-ee/eventsoutcing_pocketbase/db"
	"github.com/pocketbase/pocketbase"
	pbcore "github.com/pocketbase/pocketbase/core"
	"log"
	"sync"
	"testing"

	"github.com/hallgren/eventsourcing/core"
	"github.com/hallgren/eventsourcing/core/testsuite"
)

func TestSuite(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	appInst := &app{
		PocketBase: pocketbase.New(),
	}

	err := appInst.Bootstrap()

	if err != nil {
		t.Fatalf("Failed to bootstrap PocketBase app: %v", err)
	}

	log.Printf("Pocketbase data dir: %v\n", appInst.DataDir())

	user := db.NewUser(appInst)
	err = user.Load()
	if err != nil {
		t.Fatalf("Failed to load user: %v", err)
	}

	f := func() (store core.EventStore, closeFunc func(), err error) {
		storeCol := New(user, []string{"admin", "maintainer", "user"}, appInst)
		if err = storeCol.Load(); err != nil {
			return
		}
		store = storeCol

		closeFunc = func() {
		}
		return
	}
	testsuite.Test(t, f)
}

type app struct {
	*pocketbase.PocketBase

	Recreate     bool
	RecreateAuth bool
	AuthDisabled bool
}

func (db *app) App() pbcore.App {
	return db.PocketBase
}

func (db *app) IsRecreateDb() bool {
	return db.Recreate
}

func (db *app) IsRecreateDbAuth() bool {
	return db.RecreateAuth
}

func (db *app) IsAuthDisabled() bool {
	return db.AuthDisabled
}
