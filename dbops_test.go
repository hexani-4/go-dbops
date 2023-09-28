package dbops_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hexani-4/go-dbops"
)

func TestAddTestDelete(t *testing.T) {
	db_path := "./test.db"

	test_structure := dbops.Db_structure{"setďings": &dbops.Db_table{Columns: []string{"key TEXT", "value TEXT"}, Primary_key: []string{"key"}},
										 "tables": &dbops.Db_table{Columns: []string{"tablename TEXT", "table_columns TEXT", "table_pk TEXT"}},
  										}
	test_structure2 := dbops.Db_structure{"test": &dbops.Db_table{Columns: []string{"ey TEXT", "uy TEXT"}, Primary_key: []string{"ey"}}}

	fmt.Println("CreateDataSource")
	err := dbops.CreateDataSource(db_path, true)
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("AddDataSource")
	err = dbops.AddDataSource(db_path, "db")
	if err != nil { t.Fatalf(err.Error()) }

	_, _, err = dbops.FormatUInputTable("db.database_log")
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("ExtendDataSource")
	err = dbops.ExtendDataSource("db", test_structure)
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("CheckSourceStructure - strict")
	result, err := dbops.CheckSourceStructure("db", test_structure, true)
	if err != nil {
		t.Fatalf(err.Error())
	} else if !result {
		t.Fatalf("expected true, got false")
	}

	fmt.Println("CheckSourceStructure - not strict")
	result, err = dbops.CheckSourceStructure("db", test_structure, false)
	if err != nil {
		t.Fatalf(err.Error())
	} else if !result {
		t.Fatalf("expected true, got false")
	}

	fmt.Println("CheckTableStructure - for each table")
	for tablename, table := range test_structure {
		result, err = dbops.CheckTableStructure("db." + tablename, table)
		if err != nil {
			t.Fatalf(err.Error())
		} else if !result {
			t.Fatalf("expected true, got false") 
		}
	}

	fmt.Println("InsertData")
	err = dbops.InsertData("db.tables", "a", "e", "č")
	if err != nil { t.Fatalf(err.Error()) }

	time.Sleep(1 * time.Second)

	fmt.Println("RemoveDataSource")
	err = dbops.RemoveDataSource("db")
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("AddDataSource")
	err = dbops.AddDataSource(db_path, "db")
	if err != nil { t.Fatalf(err.Error()) }

	fmt.Println("ExtendDataSource")
	
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