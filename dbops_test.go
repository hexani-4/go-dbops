package dbops_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/hexani-4/go-dbops"
)

// tests that all non-memory functions do what they should in the most basic of cases, this is a sanity check, not meant to show anything
func TestBasic(t *testing.T) {

	//init
	var wd, _ = os.Getwd()
	var db_path = filepath.Join(wd, "test.db")
	var sec_path = filepath.Join(wd, "secondary.db")

	var tts = []dbops.Table{{Name: "te st ", Dd: true, Cols: []dbops.Col{{Name: "a a", Ext: "INTEGER", Pk: true},
		{Name: "b b", Ext: "TEXT", Pk: true},
		{Name: "c c", Ext: "REAL", Pk: false}}},
		{Name: "šeež¨", Dd: false, Cols: []dbops.Col{{Name: " á ", Ext: "INTEGER", Pk: true}}}}

	var tdata = [][]any{{int64(0), "0", 0.0}, {int64(1), "1", 1.0}, {int64(2), "2", 2.0}}

	fmt.Println("sanity check with database at", db_path, "\n    and", sec_path)
	var src *dbops.DataSrc
	var err error
	_, err = os.Stat(db_path)
	if !os.IsNotExist(err) {
		os.Remove(db_path)
	}
	_, err = os.Stat(sec_path)
	if !os.IsNotExist(err) {
		os.Remove(sec_path)
	}

	//create source
	src, err = dbops.CreateSrc(db_path, tts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//check that all tables exist
	err = checkTables(src, tts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//disconnect src
	err = src.Disconnect()
	if err != nil {
		t.Fatalf(err.Error())
	}

	//reconnect to src
	src, err = dbops.ConnectSrc(db_path, true)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//recheck that all tables exist
	err = checkTables(src, tts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//remove tts[0]
	err = src.DelTable(src.GetRtable(tts[0].Name))
	if err != nil {
		t.Fatalf(err.Error())
	}

	//check that it's gone
	err = checkTables(src, tts[1:])
	if err != nil {
		t.Fatalf(err.Error())
	}

	//add tts[0]
	err = src.AddTable(tts[0])
	if err != nil {
		t.Fatalf(err.Error())
	}

	//we removed [0] from the left, then added it to the right, meaning the real tables are now in the inverse order
	slices.Reverse(tts)

	//check that it exists again
	err = checkTables(src, tts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//make sure release/reclaim works properly
	src.Release()
	err = src.Reclaim(false, true)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//get tts[1] as an rtable (the originally first test table)
	tbl := src.GetRtable(tts[1].Name)

	//insert tdata[0] && tdata[1]
	err = tbl.InsertData(dbops.Conf_abort, append(tdata[0], tdata[1]...))
	if err != nil {
		t.Fatalf(err.Error())
	}

	//check that it got inserted
	err = checkData(tbl, tdata[:2], false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//prepare args for editing the table + add the new column to test data
	remap := make(map[string]string, len(tts[1].Cols))
	for _, col := range tts[1].Cols {
		remap[col.Name] = col.Name
	}

	tts[1].Cols = append(tts[1].Cols, dbops.Col{Name: "d d", Ext: "INTEGER", Pk: true})

	for i, row := range tdata {
		tdata[i] = append(row, nil)
	}

	//edit the table
	err = tbl.Edit(tts[1].Cols, remap)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//check that it was edited properly
	err = checkData(tbl, tdata[:2], false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//dd the first row of test data
	err = tbl.DeleteData([]dbops.Condition{{Cname: tts[1].Cols[0].Name, Op: dbops.Op_eq, Val: tdata[0][0]}})
	if err != nil {
		t.Fatalf(err.Error())
	}

	//check that it will not be returned
	err = checkData(tbl, tdata[1:2], true)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//undo the deletion of the first row of test data
	err = tbl.UndoDelete(1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//check that it will be returned again
	err = checkData(tbl, tdata[:2], false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	nodd_tt := tts[1]
	nodd_tt.Dd = false
	//create a dummy database (with only the first table)
	dest, err := dbops.CreateSrc(sec_path, []dbops.Table{nodd_tt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	//fetch data from src to the dummy
	err = dest.FetchAllFrom(src, dbops.Conf_abort, false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	dtbl := dest.GetRtable(nodd_tt.Name)
	if dtbl == nil {
		t.Fatalf("nil destination table")
	}

	//check that all data in tts[1] of src is now in dummy
	err = checkData(dtbl, tdata[:2], false)
	if err != nil {
		t.Fatalf(err.Error())
	}

	//cleanup
	err = src.Disconnect()
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = dest.Disconnect()
	if err != nil {
		t.Fatalf(err.Error())
	}

	os.Remove(db_path)
	os.Remove(sec_path)
}

/*
All mem funcs
*/

func checkTables(src *dbops.DataSrc, expected_tables []dbops.Table) error {

	//individually check tablenames, then table structures
	for _, tt := range expected_tables {
		if !src.HasTableOfName(tt.Name) {
			return fmt.Errorf("individual tablename check error ( %s )", tt.Name)
		}

		if !src.HasTable(tt) {
			return fmt.Errorf("individual table schema check error ( %s )", tt.Name)
		}

		//also check rtables (and their conversion to standard ones)
		if !reflect.DeepEqual(src.GetRtable(tt.Name).ToTable(), tt) {
			return fmt.Errorf("rtable schema conversion error ( %s )", tt.Name)
		}
	}

	//batch check tablenames, then table structures
	etns := make([]string, len(expected_tables))
	for i, et := range expected_tables {
		etns[i] = et.Name
	}

	if !reflect.DeepEqual(src.GetTableNames(), etns) {
		return fmt.Errorf("batch tablename check error")
	}

	if !reflect.DeepEqual(src.GetTables(), expected_tables) {
		return fmt.Errorf("batch table schema check error")
	}

	return nil
}

func checkData(tbl *dbops.Rtable, expected_data [][]any, nocount bool) error {

	if !nocount {
		//check table length
		num := tbl.Count()
		if num == -1 {
			return fmt.Errorf("failed to count rows of table ( %s )", tbl.ToTable().Name)
		}
		if num != len(expected_data) {
			fmt.Println("<check data> expected length", len(expected_data), "received", num)
			return fmt.Errorf("table row count error ( %s )", tbl.ToTable().Name)
		}
	}

	//get the actual data
	rdata, err := tbl.GetData(0, -1, []dbops.Condition{{Ljoin: true, Lrel: false, Cname: "a a", Op: dbops.Op_neq, Val: 69}}, []dbops.Order{{Cname: "a a", Dir: false, Nullwh: false}})
	if err != nil {
		return err
	}

	if len(rdata) != len(expected_data) {
		return fmt.Errorf("data length error ( %s )", tbl.ToTable().Name)
	}

	//check actual-expected data equality (row-wise)
	for i, rrw := range rdata {
		erw := expected_data[len(expected_data)-i-1]

		if !slices.Equal(rrw, erw) {
			fmt.Println("<check data> expected", rrw, "received", erw)
			return fmt.Errorf("data mismatch ( %s | row %d )", tbl.ToTable().Name, i)
		}
	}

	return nil
}
