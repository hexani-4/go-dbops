package dbops_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hexani-4/go-dbops"
)

func TestAddTestDelete(t *testing.T) {
	db_path := "./test.db"

	test_structure := dbops.Db_structure{"setďings": &dbops.Db_table{Columns: []dbops.Db_col{{Name: "key", Ext: "TEXT"}, {Name: "value", Ext: "TEXT"}}, Primary_key: []string{"key"}},
										 "ta bl es ": &dbops.Db_table{Columns: []dbops.Db_col{{Name: "table name", Ext: "TEXT"}, {Name: "table_columns", Ext: "TEXT"}, {Name: "table_pk", Ext: "TEXT"}}},
  										}
	test_structure2 := dbops.Db_structure{"test": &dbops.Db_table{Columns: []dbops.Db_col{{Name: "ey", Ext: "TEXT"}, {Name: "uy", Ext: "TEXT"}}, Primary_key: []string{"ey"}}}

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
	for tablename, table := range test_structure {
		fmt.Println(tablename, *table)
	}
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
	err = dbops.InsertData("db.ta bl es ", "a", "e", "č")
	if err != nil { t.Fatalf(err.Error()) }


	fmt.Println("GetDataByIndex")
	data, err := dbops.GetDataByIndex("db.ta bl es ", "", 0, -1)
	fmt.Println(data)
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