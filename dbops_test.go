package dbops_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hexani-4/go-dbops"
)

func TestAddTestDelete(t *testing.T) {
	db_path := "./test.db"

	fmt.Println("CreateDataSource")
	err := dbops.CreateDataSource(db_path, true)
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("AddDataSource")
	err = dbops.AddDataSource(db_path, "db")
	if err != nil { t.Fatalf(err.Error()) }

	_, _, err = dbops.FormatUInputTable("db.database_log")
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("ExtendDataSource")
	test_structure := dbops.Db_structure{"settings": &dbops.Db_table{Columns: []string{"key TEXT", "value TEXT"}, Primary_key: []string{"key"}},
 										 "sure": &dbops.Db_table{Columns: []string{"key TEXT", "value TEXT"}, Primary_key: []string{"key"}},
										}
	err = dbops.ExtendDataSource("db", test_structure)
	if err != nil { t.Fatalf(err.Error()) }

	time.Sleep(1 * time.Second)

	fmt.Println("RemoveDataSource")
	err = dbops.RemoveDataSource("db")
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("AddDataSource")
	err = dbops.AddDataSource(db_path, "db")
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("ExtendDataSource")
	test_structure2 := dbops.Db_structure{"test": &dbops.Db_table{Columns: []string{"ey TEXT", "uy TEXT"}, Primary_key: []string{"ey"}}}
	err = dbops.ExtendDataSource("db", test_structure2)
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("Load things")

	fmt.Println("RemoveDataSource")
	err = dbops.RemoveDataSource("db")
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("AddDataSource")
	err = dbops.AddDataSource(db_path, "db")
	if err != nil { t.Fatalf(err.Error()) }

	time.Sleep(10 * time.Second)

	fmt.Println("DeleteDataSource")
	err = dbops.DeleteDataSource("db")
	if err != nil { t.Fatalf(err.Error()) }
}