package dbops_test

import (
	"testing"
	"time"

	"github.com/hexani-4/go-dbops"
)

func TestAddTestDelete(t *testing.T) {
	db_path := "./test.db"

	err := dbops.CreateDataSource(db_path, false)
	if err != nil { t.Fatalf(err.Error()) }


	err = dbops.AddDataSource(db_path, "db")
	if err != nil { t.Fatalf(err.Error()) }


	time.Sleep(20 * time.Second)

	err = dbops.DeleteDataSource("db")
	if err != nil { t.Fatalf(err.Error()) }
}