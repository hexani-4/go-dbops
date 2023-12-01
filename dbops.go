package dbops

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

/*
MAJOR FOOTGUNS:
 -	 if you have a Rtable of name "test", and a Rtable of name "test_transitionstate", tbl.Edit() (and all of its siblings) will throw an error from sqlx
 -	 don\"t use CreateMem() in async code, could cause a race cond -> error
*/

//TODO: missing logging in a bunch of functions, consider removing logging alrogether

const transtate_marker string = "_transitionstate"

var ErrUnknown error = errors.New("ErrUnknown (dbops) - An unspecified error was thrown by something which shouldn\"t throw errors, check what you passed in")

var ErrIsNotDatabase error = errors.New("ErrIsNotDatabase (dbops) - The supplied filepath/file load did not end in .db, check for typo in extension")

var ErrNilSource error = errors.New("ErrNilSource (dbops) - Cannot perform function on nil (as *dataSource)")
var ErrInvalidTable error = errors.New("ErrInvalidTable (dbops) - The supplied value is not compatible")

var ErrNoMem error = errors.New("ErrNoMem (dbops) - cannot perform in-memory functions without an in-memory database")

var ErrIsDuplicate error = errors.New("ErrIsDuplicate (dbops) - Attempted to create two items with the same name/properties")
var ErrIsNotPresent error = errors.New("ErrIsNotPresent (dbops) - Attempted to operate on something not present in the target")

var ErrDiffStructure error = errors.New("ErrDiffStructure (dbops) - Attempted to operate on differently structured items with a function which does not resolve dissimilarities")
var ErrMemSchemaDesync error = errors.New("ErrMemSchemaDesync (dbops) - A data source\"s in-memory database is not of the same schema as its on-disk one")

var ErrBadData error = errors.New("ErrBadData (dbops) - The requested operation is impossible with the supplied data")


type empty struct{}
type stringset map[string]empty
	//true if r is in set
	func (set stringset)has(s string) (b bool) {
		_, ok := set[s]
		return ok
	}

type OrdCr struct {
	ColNames []string //names of the columns to order by, in order of importance
	Order bool //true = ascending, false = descending
}


type logRow struct {
	unixmilli int //milliseconds since epoch of when this event was logged
	event logevent //what happened
	owner string //what this action is related to
	details string //additional info
}

type Table struct {
	Name string
	Dd bool
	Cols []Col
}

type Col struct {
	Name string //Name (any string you want)
	Ext string //Datatype ("TEXT", "INTEGER DEFAULT 0 NOT NULL", etc...)
	Pk bool //Whether this column is a part of the Primary Key
}

type rcol struct {
	name string //Name (any string you want)
	ext string //Datatype ("TEXT", "INTEGER DEFAULT 0 NOT NULL", etc...)
	pk bool //Whether this column is a part of the Primary Key
}

type Rtable struct {
	name string //The string you will address this Rtable with 

	dd bool //Whether this Rtable uses deltaDelete
	ddint int //integer representation of dd

	parent *DataSrc //the data source this Rtable belongs to

	cols []rcol
}

type DataSrc struct {
	dlock chan bool
	memlock chan bool

	path string
	db *sqlx.DB
	mem *sqlx.DB
	
	log bool //Whether this database is logged
	rtables []*Rtable
}



//Rtable that will be created/present if src.log = true ; will always be leftmost in src.rtables
var orgLogTbl = &Rtable{name: "databaseLog", dd: false, ddint: 0, cols: []rcol{{name: "unixmilli", ext: "INTEGER", pk: false}, 
																			   {name: "event",     ext: "TEXT",    pk: false}, 
																		  	   {name: "owner",     ext: "TEXT",    pk: false}, 
																	    	   {name: "info",      ext: "TEXT",    pk: false}}}


//column that will be created/present if Rtable.dd == true ; will always be leftmost in Rtable.cols
var orgDdCol = rcol{name: "verIndex", ext: "INTEGER DEFAULT 0 NOT NULL", pk: false}

type Condarr []Condition
type Condition struct {
	Ljoin bool //true -> (everything <and> before) <or> <this WhereCond>
	Lrel bool //true -> and ; false -> or (with the previous WhereCond)

	Cname string //name of the column to operate on (unchecked string literal)
	Op operator
	Val any //value to compare to (parsed by sqlx)
}
var orgDdColIs0 Condition = Condition{Ljoin: true, Lrel: true, Cname: orgDdCol.name, Op: Op_eq, Val: 0}

type Ordarr []Order //most important comes first

type Order struct {
	Cname string //name of the col
	Dir bool //direction to order in (false -> DESC ; true -> ASC)
	Nullwh bool //how to order null values (false -> last ; true -> first)
}

// -------------------- DATABASE CONSTANT DEFS --------------------

type operator string //constants begin with "Op_"; conditions for creating very simple "where" logic
const(
	Op_eq operator = " == " //equal
	Op_neq operator = " != " //not equal

	Op_is operator = " IS NOT DISTICT FROM " //equal, but will match two "NULL"(s)
	Op_isnot operator = " IS DISTINCT FROM " //not equal, but will consider two "NULL"(s) equal

	Op_less operator = " < " //less than
	Op_more operator = " > " //more than

	Op_eqless operator = " <= " //less than or equal
	Op_eqmore operator = " >= " //more than or equal
)

type conflict_behaviour string //constants begin with "conf_"; conf_rollback, conf_abort and conf_fail result in one of the sqlite.ErrConstraint...... errors on activation
const (
	Conf_rollback conflict_behaviour = "ROLLBACK" //aborts the failed SQL statement and rolls back the current transaction. 
	Conf_abort    conflict_behaviour = "ABORT" //aborts the failed SQL statement (and any changes it caused), but keeps changes made by previous statements of this transaction.
	Conf_fail     conflict_behaviour = "FAIL" //stops the failed SQL statement, but keeps any changes it caused and keeps changes made by previous statements of this transaction.
	Conf_ignore   conflict_behaviour = "IGNORE" //skips the failed SQL statement, does not return errors. (except foreign key constraint violations -> conf_abort)
	Conf_replace  conflict_behaviour = "REPLACE" //deletes all preexisting rows which caused this conflict, and continues with the SQL statement (except inserting null into not-null columns without default values -> conf_abort)
)

type logevent string //constants begin with "log_"
const(
	//no event -> details = anything, really
	log_noevent logevent = "none"

	//Rtable creation/deletion -> owner = tablename ; no details
	log_createT logevent = "Rtable creation"
	log_removeT logevent = "Rtable deletion"

	//database merging -> details = <other_src>.path
	log_takedata logevent = "copied data of"
	log_givedata logevent = "data copied by"

	//dbops connect/disconnect -> no details
	log_connDisk    logevent = "dbops connected"
	log_disconnDisk logevent = "dbops disconnected"

	//dbops make/destroy in-memory db -> no details
	log_connMem    logevent = "created in-memory db"
	log_disconnMem logevent = "deleted in-memory db"

	//Rtable schema change -> details = schema before the change, in a human-readable-ish format
	log_editT   logevent = "schema change from"

	//Rtable write -> details = num. of rows
	log_writeT  logevent = "write"

	//Rtable read -> details = num. of rows
	log_readT   logevent = "read"
)




// -------------------- GENERAL PURPOSE FUNCTIONS --------------------
	
/*
Checks if two Rtable(s) are equal (in Rtable.name && Rtable.cols)
 -	 two tables with the same columns, but different names, will not be equal
 -	 two tables with the same columns (not counting dd), except for one having Rtable.dd == true and the other one false, will be equal
 -	 two tables which are both nil will NOT be equal
 */
func rtEqual(t *Rtable, t2 *Rtable) bool {
	if (t == nil) || (t2 == nil) { return false }
	if t.name != t2.name { return false }
	
	if (t.dd && t2.dd) || (!t.dd && !t2.dd) {
		return slices.Equal(t.cols, t2.cols)

	} else if t.dd {
		return slices.Equal(t.cols[1:], t2.cols)

	} else { //-> t2.dd == true
		return slices.Equal(t2.cols[1:], t.cols)
	}
} 

//Checks if two tables are exactly equal in all fields except <t>.parent (nil and nil will not be equal)
func rtStrictEqual(t *Rtable, t2 *Rtable) bool {
	if (t == nil) || (t2 == nil) { return false }
	if t.name != t2.name { return false }
	if t.dd != t2.dd { return false }

	return slices.Equal(t.cols, t2.cols)
}

func pubPriColEqual(c Col, rc rcol) bool {
	if (c.Name != rc.name) ||
	   (c.Ext != rc.ext) || 
	   (c.Pk != rc.pk) { return false }

	return true
}




func (t Table)equal(rt *Rtable) bool {
	if (rt == nil) { return false }
	if t.Name != rt.name { return false }
	
	if rt.dd { //Table cannot have a dd column
		return slices.EqualFunc(t.Cols, rt.cols[1:], pubPriColEqual)
	} else {
		return slices.EqualFunc(t.Cols, rt.cols, pubPriColEqual)
	}
}


func (t Table)valid() bool {
	if t.Name == "" { return false }

	cnameset := make(stringset, len(t.Cols))
	for _, c := range t.Cols {

		if c.Name == "" { return false }
		if cnameset.has(c.Name) { return false }
		//I should probably also check if c.Ext is valid, buuut...https://www.sqlite.org/syntax/column-constraint.html

		cnameset[c.Name] = empty{}
	}
	return true
}

func (rt *Rtable)valid() bool {
	return (rt != nil) || (rt.parent != nil)
}


func (rt *Rtable)ToTable() Table {
	t := Table{}
	if !rt.valid() { return t }
	
	t.Name = rt.name
	t.Dd = rt.dd

	t.Cols = make([]Col, len(rt.cols) - rt.ddint)
	for i, c := range rt.cols[rt.ddint:] { t.Cols[i] = Col{Name: c.name, Ext: c.ext, Pk: c.pk} } 

	return t
}

// -------------------- OPERATIONS REGARDING DATA SOURCES --------------------

//tries to create a database at <path> (will not overwrite), adds <tables> to it, and returns a handle to it
func CreateSrc(path string, tables []Table) (*DataSrc, error) {
	var src DataSrc
	var err error

	_, err = os.Stat(path)
	if err == nil { err = os.ErrExist }
	if !os.IsNotExist(err) { return nil, err }
	dirpath, fname := filepath.Split(path)
	if !(filepath.Ext(fname) == ".db") { return nil, ErrBadData}

	src.rtables = make([]*Rtable, len(tables))

	for i, t := range tables {
		if !t.valid() { return nil, ErrInvalidTable } //convert Table -> Rtable

		new_rt := Rtable{name: t.Name, dd: t.Dd}
		if new_rt.dd { new_rt.ddint = 1 }

		new_rt.cols = make([]rcol, len(t.Cols) + new_rt.ddint)
		if new_rt.dd { new_rt.cols[0] = orgDdCol }

		for i, c := range t.Cols {
			new_rt.cols[i + new_rt.ddint] = rcol{name: c.Name, ext: c.Ext, pk: c.Pk}
		}

		new_rt.parent = &src
		src.rtables[i] = &new_rt
	}

	err = os.MkdirAll(dirpath, 0700)
	if (err != nil) { return nil, err }

	src.db, err = sqlx.Connect("sqlite3", path)
	if (err != nil) { return nil, err }
	src.path = path

	for _, rt := range src.rtables {
		statement := rt.crStatement()
		_, err = src.db.Exec(statement)
		if err != nil { return nil, err }
	}

	src.dlock = make(chan bool, 1)
	src.dlock <- true
	return &src, nil
}

//tries to return a handle to the database at <path> (seekOwn -> will seek a log table and Dd columns) 
func ConnectSrc(path string, seekOwn bool) (*DataSrc, error) {
	var src DataSrc
	var err error

	_, err = os.Stat(path)
	if err != nil { return nil, err }

	if filepath.Ext(path) != ".db" { return nil, ErrIsNotDatabase }

	src.db, err = sqlx.Connect("sqlite3", path)
	if err != nil { return nil, err }
	src.path = path

	tables, hasLog, err := src.realTables(false, seekOwn)
	if err != nil { return nil, err }
	src.log = hasLog
	src.rtables = tables

	src.logEvent(log_connDisk, orgLogTbl.name, "")

	src.dlock = make(chan bool, 1)
	src.dlock <- true
	return &src, nil
}

//tries to return tables of <src> actually present on-disk (seekOwn -> will seek a log table and Dd columns)
func (src *DataSrc)realTables(fdisk_tmem bool, seekOwn bool) (tables []*Rtable, hasLog bool, err error) {
	if src == nil { return []*Rtable{}, false, ErrNilSource}

	var db *sqlx.DB
	if fdisk_tmem {
		db = src.mem
		if db == nil { return []*Rtable{}, false, ErrNoMem }
	} else {
		db = src.db
	}

	var tblname_list []string
	err = db.Select(&tblname_list, "SELECT name FROM sqlite_master WHERE type=\"table\";")
	if err != nil { return tables, hasLog, err }

	for _, tablename := range tblname_list {
		cols, err := db.Queryx(fmt.Sprintf("PRAGMA table_info(\"%s\");", tablename))
		if err != nil { return tables, hasLog, err }
		defer cols.Close()

		tbl := Rtable{parent: src, name: tablename}
		e := 0
		for cols.Next() {
			var index int
			var name string
			var datatype string
			var allows_null bool
			var default_value sql.NullString
			var pk int
		
			err := cols.Scan(&index, &name, &datatype, &allows_null, &default_value, &pk)
			if err != nil { return tables, hasLog, err }

			var extra_props string  
			if default_value.Valid { extra_props += " DEFAULT " + default_value.String }
			if allows_null { extra_props += " NOT NULL" }

			rcol := rcol{name: name, ext: datatype + extra_props}
			if pk != 0 {rcol.pk = true}

			//checks for a DeltaDelete rcol
			if seekOwn && (rcol == orgDdCol) {
				tbl.dd = true
				tbl.ddint = 1
			}

			tbl.cols = append(tbl.cols, rcol)
			e++
		}

		//checks for a compatible databaseLog Rtable
		if seekOwn && rtStrictEqual(&tbl, orgLogTbl) { 
			hasLog = true
			tables = append([]*Rtable{&tbl}, tables...)

		} else {
			tables = append(tables, &tbl)
		}	
	}
	return tables, hasLog, nil
}

//invalidates <src>, tries to properly free all of its resources
func (src *DataSrc)Disconnect() (err error) {
	if src == nil { return ErrNilSource }
	
	var wg sync.WaitGroup
	wg.Add(2)
		go func(){ <- src.dlock ; wg.Done() }()
		go func(){ if src.mem != nil { <- src.memlock } ; wg.Done() }()
	wg.Wait()

	src.logEvent(log_disconnDisk, orgLogTbl.name, "")

	delfunc := func(db *sqlx.DB, errout chan error){ defer wg.Done()
		if db == nil { return }

		err = src.db.Close()
		if err != nil { errout <- err ; return }
	}

	errin := make(chan error, 2)
	wg.Add(2)
		go delfunc(src.db, errin)
		go delfunc(src.mem, errin)
	wg.Wait()
	src.db, src.mem = nil, nil
	if len(errin) != 0 { return <- errin }

	return nil
}




//tries to create <t> in <src>
func (src *DataSrc)AddTable(t Table) (error) {
	if src == nil { return ErrNilSource }
	if !t.valid() { return ErrInvalidTable }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- src.dlock ; wg.Done() }()
		go func() {	if src.mem != nil { <- src.memlock } ; wg.Done() }()
	wg.Wait()

	defer func() { 
		src.dlock <- true
		if src.mem != nil { src.memlock <- true }	
	}()
		
	if !t.valid() { return ErrInvalidTable } 
	for _, rt := range src.rtables { //check if table of this name does not already exist
		if rt.name == t.Name { return ErrIsDuplicate }
	}

	//convert Table -> Rtable
	new_rt := Rtable{name: t.Name, dd: t.Dd}
	if new_rt.dd { new_rt.ddint = 1 }

	new_rt.cols = make([]rcol, len(t.Cols) + new_rt.ddint)
	if new_rt.dd { new_rt.cols[0] = orgDdCol }

	for i, c := range t.Cols {
		new_rt.cols[i + new_rt.ddint] = rcol{name: c.Name, ext: c.Ext, pk: c.Pk}
	}

	statement := new_rt.crStatement()

	addfunc := func(db *sqlx.DB, errout chan error){ defer wg.Done()
		if db == nil { return }

		_, err := src.db.Exec(statement)
		if err != nil { errout <- err ; return }
	}

	errin := make(chan error, 2)
	wg.Add(2)
		go addfunc(src.db, errin)
		go addfunc(src.mem, errin)
	wg.Wait()
	if len(errin) != 0 { return <- errin }

	new_rt.parent = src
	src.rtables = append(src.rtables, &new_rt)
	src.logEvent(log_createT, new_rt.name, "")

	return nil
}

//tries to delete <t> in <src>
func (src *DataSrc)DelTable(rt *Rtable) (error) {
	if src == nil { return ErrNilSource }
	if !rt.valid() { return ErrInvalidTable }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- src.dlock ; wg.Done() }()
		go func() {	if src.mem != nil { <- src.memlock } ; wg.Done() }()
	wg.Wait()

	defer func(){ 
		src.dlock <- true
		if src.mem != nil { src.memlock <- true }	
	}()
	
	if !(rt.parent == src) { return ErrIsNotPresent }

	var tidx int
	for i, srct := range src.rtables { if rt == srct { tidx = i ; break } }

	dropfunc := func(db *sqlx.DB, errout chan error){ defer wg.Done()
		if db == nil { return }

		_, err := db.Exec("DROP TABLE \"main\".\"" + rt.name + "\";")
		if err != nil { errout <- err ; return }
	}

	errin := make(chan error, 2)
	wg.Add(2)
		go dropfunc(src.db, errin)
		go dropfunc(src.mem, errin)
	wg.Wait()
	if len(errin) != 0 { return <- errin }

	src.rtables = append(src.rtables[:tidx], src.rtables[tidx+1:]...) 
	src.logEvent(log_removeT, rt.name, "")
	return nil
}

//without validating <t>, returns an sql statement for its creation
func (t *Rtable)crStatement() (statement string) {
	coldefs := make([]string, len(t.cols))
	var pkdefs []string
	for e, rcol := range t.cols {
		coldefs[e] = "\"" + rcol.name + "\" " + rcol.ext

		if rcol.pk { pkdefs = append(pkdefs, "\"" + rcol.name + "\"") }
	}

	if len(pkdefs) == 0 {
		statement = "CREATE TABLE \"" + t.name + "\"(" + strings.Join(coldefs, ", ") + ");"
	} else {
		statement = "CREATE TABLE \"" + t.name + "\"(" + strings.Join(coldefs, ", ") + ", PRIMARY KEY(" + strings.Join(pkdefs, ", ") + "));"
	}
	return statement
}




//tries to create an in-memory datbase for <src> (does nothing if it already exists)
func (src *DataSrc)CreateMem() (err error) {
	if src == nil { return ErrNilSource }

	if src.mem != nil { return nil }

	src.mem, err = sqlx.Connect("sqlite3", ":memory:")
	if err != nil { return err }

	src.logEvent(log_connMem, orgLogTbl.name, "")

	for _, tbl := range src.rtables {
		statement := tbl.crStatement()
		_, err := src.mem.Exec(statement)
		if err != nil { return err }
	}

	src.memlock = make(chan bool, 1)
	src.memlock <- true
	return nil
}

//tries to insert (or <cbh>) all rows of <src>\"s in-memory database into disk
func (src *DataSrc)SaveMem(cbh conflict_behaviour) (err error) {
	if src == nil { return ErrNilSource }
	if src.mem == nil { return ErrNoMem }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- src.dlock ; wg.Done() }()
		go func() {	<- src.memlock ; wg.Done() }()
	wg.Wait()

	defer func() { 
		src.dlock <- true
		src.memlock <- true 	
	}()

	_, err = src.mem.Exec("ATTACH DATABASE \"" + src.path + "\" AS \"disk\";")
	if err != nil { return err }
	defer src.mem.Exec("DETACH DATABASE \"disk\";")

	for _, t := range src.rtables {
	
		_, err = src.mem.Exec("INSERT OR " + string(cbh) + " INTO \"disk\".\"" + t.name + "\" SELECT * FROM \"main\".\"" + t.name + "\";")
		if err != nil { return err }
	}
	return nil
}

//tries to delete <src>\"s in-memory database (without saving)
func (src *DataSrc)DeleteMem() (err error) {
	if src == nil { return ErrNilSource }
	if src.mem == nil { return ErrNoMem }

	<- src.memlock

	err = src.mem.Close()
	if err != nil { return err }
	src.mem = nil

	return nil
}




//forces other methods of <src> to block until you <src>.Reclaim() it, but exposes you its underlying database handles 
func (src *DataSrc)Release() (disk *sqlx.DB, mem *sqlx.DB) {
	if src == nil { return nil, nil }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() {	<- src.dlock ; wg.Done() }()
		go func() { if src.mem != nil { <- src.memlock } ; wg.Done() }()
	wg.Wait()

	return src.db, src.mem
}

/*
tries to let other methods of <src> execute as normal (any err -> forced method block stays)
 -	 if <noreload> is false, rechecks <src>\"s schema (seekOwn -> will seek a log table and Dd columns)
note - calling this is to mean that you are done using <src>\"s exposed database handles
*/
func (src *DataSrc)Reclaim(noreload bool, seekOwn bool) (error) {
	if src == nil { return ErrNilSource }

	if !noreload {
		var rmemtables []*Rtable
		var memlog bool

		var derr error
		var merr error
		var wg sync.WaitGroup
		wg.Add(2)
			go func() { defer wg.Done() //gen disk structure
				src.rtables, src.log, derr = src.realTables(false, seekOwn)
			}()

			go func() { defer wg.Done() //gen mem structure
				if src.mem != nil { rmemtables, memlog, merr = src.realTables(true, seekOwn) }
			}()
		wg.Wait()

		if derr != nil { return derr }
		if merr != nil { return merr }
		
		if (src.log != memlog) || slices.EqualFunc(src.rtables, rmemtables, rtStrictEqual) { return ErrMemSchemaDesync }
	}

	src.dlock <- true
	if src.mem != nil { src.memlock <- true }
	return nil
}




//if <src>.log, adds a log with a timestamp of millis since epoch
func (src *DataSrc)logEvent(event logevent, owner string, details string) {
	if src == nil { return }

	if !src.log { return }

	timestamp := time.Now().UnixMilli()
	src.db.Exec("INSERT OR REPLACE INTO \"main\".\"databaseLog\" VALUES(?, ?, ?, ?)", timestamp, owner, event, details)
}

//returns all logs of <src> where event is <event> and owner is <owner> (owner == "" -> any owner)
func (src *DataSrc)getLogs(event logevent, owner string) (logRows []logRow) {
	if src == nil { return logRows }

	if !src.log { return logRows }

	var rows *sqlx.Rows
	var err error

	query := "SELECT \"timestamp\", \"event, \"owner\", \"details\" FROM \"main\".\"databaseLog\" WHERE event = ?"
	if owner == "" { 
		query += " ORDER BY timestamp DESC;"
		rows, err = src.db.Queryx(query, string(event))
	} else {
		query += " AND owner = ? ORDER BY timestamp DESC;"
		rows, err = src.db.Queryx(query, string(event), owner)
	}
	if err != nil { return logRows }
	defer rows.Close()

	for rows.Next() {
		var logRow logRow
		var unconv_event string
		rows.Scan(&logRow.unixmilli, &unconv_event, &logRow.owner, &logRow.details) //ignores error
		logRow.event = logevent(unconv_event)

		logRows = append(logRows, logRow)
	}
	return logRows
}




/*
tries to insert (or <cbh>) all rows of all tables in <src> into their counterparts in <dest> (mustAll -> if no counterpart, abort)
 -	 log and dd don't have to match
 -	 operates on-disk only
*/
func (dest *DataSrc)FetchAllFrom(src *DataSrc, cbh conflict_behaviour, mustAll bool) (err error) {
	if (dest == nil) || (src == nil) { return ErrNilSource }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- src.dlock ; wg.Done() }()
		go func() {	<- dest.dlock ; wg.Done() }()
	wg.Wait()

	defer func() { 
		src.dlock <- true
		dest.dlock <- true 
	}()

	if mustAll && (len(dest.rtables) != len(src.rtables)) { return ErrDiffStructure } 
	
	var tocpy []*Rtable

	//skip log table where it is if both don\"t have it
	bothLog := (dest.log && src.log) 
	var destlogint int
	var srclogint int
	if !bothLog {
		if dest.log { 
			destlogint = 1 
		} else if src.log {
			srclogint = 1
		}
	}
	
	destrtmap := make(map[string]*Rtable, len(dest.rtables)) 
	for _, rt := range dest.rtables[destlogint:] { 
		destrtmap[rt.name] = rt
	}

	for _, rt := range src.rtables[srclogint:] { 
		destrt, ok := destrtmap[rt.name]

		switch mustAll {
		case true:
			if !ok || !rtEqual(destrt, rt) { return ErrDiffStructure }

		default:
			if !ok || !rtEqual(destrt, rt) { continue }
		}
		tocpy = append(tocpy, rt)
	}

	_, err = dest.db.Exec("ATTACH DATABASE \"" + src.path + "\" AS \"src\";")
	if err != nil { return err }
	defer dest.db.Exec("DETACH DATABASE \"src\";")

	for _, srcrt := range tocpy {
		destrt := destrtmap[srcrt.name]

		var cpyddint int
		if (destrt.dd && srcrt.dd) { cpyddint = 1 }

		colnames := make([]string, len(destrt.cols) - destrt.ddint + cpyddint)
		
		if cpyddint == 1 { colnames[0] = orgDdCol.name }
		for i, c := range destrt.cols[destrt.ddint:] {
			colnames[i + cpyddint] = "\"" + c.name + "\""
		}

		jnd_cnames := strings.Join(colnames, ", ")
		_, err := dest.db.Exec("INSERT OR " + string(cbh) + " INTO \"main\".\"" + srcrt.name + "\"(" + jnd_cnames + ") SELECT " + jnd_cnames + " FROM \"src\".\"" + srcrt.name + "\";")
		if err != nil { return err }
	}
	src.logEvent(log_givedata, "", dest.path)
	dest.logEvent(log_takedata, "", src.path)
	return nil
}




//returns all tables of src, except for the log, if present
func (src *DataSrc)GetTables() []Table {
	if src == nil { return []Table{} }

	<- src.dlock
	defer func() { src.dlock <- true }()

	var srclogint int //remove the databaseLog Rtable
	if src.log { srclogint = 1 }
	
	tables := make([]Table, len(src.rtables) - srclogint)
	for i, rt := range src.rtables[srclogint:] {
		tables[i] = rt.ToTable()
	}
	return tables
}

//returns the names of all tables of src, except for the log, if present
func (src *DataSrc)GetTableNames() []string {
	if src == nil { return []string{} }

	<- src.dlock
	defer func() { src.dlock <- true }()

	var srclogint int //remove the databaseLog Rtable
	if src.log { srclogint = 1 }
	
	rtnames := make([]string, len(src.rtables) - srclogint)
	for i, rt := range src.rtables[srclogint:] {
		rtnames[i] = rt.name
	}
	return rtnames
}

//returns a handle to <tablename> (not found -> nil), cannot return the log
func (src *DataSrc)GetRtable(tablename string) *Rtable {
	if src == nil { return nil }

	<- src.dlock
	defer func() { src.dlock <- true }()

	if (tablename == orgLogTbl.name) && src.log { return nil }

	for _, tbl := range src.rtables {
		if tablename  == tbl.name { return tbl }
	} 
	return nil
}

//returns whether <src> has a table of name <tablename>
func (src *DataSrc)HasTableOfName(tablename string) bool {
	if src == nil { return false }

	<- src.dlock
	defer func() { src.dlock <- true }()

	for _, rt := range src.rtables {
		if tablename  == rt.name { return true }
	} 
	return false
}

//returns whether <src> has <t> (Dd doesn\"t have to match)
func (src *DataSrc)HasTable(t Table) bool {
	if src == nil { return false }

	<- src.dlock
	defer func() { src.dlock <- true }()

	for _, rt := range src.rtables {
		if t.equal(rt) { return true }
	}
	return false
}


// -------------------- OPERATIONS REGARDING TABLES --------------------

//tries to recreate <rt> (both disk and memory) with only <newcols>, while copying data from old columns (key) into new ones (value) according to <remap>
func (rt *Rtable)Edit(newcols []Col, remap map[string]string) (error) {
	if !rt.valid() { return ErrInvalidTable }

	var wg sync.WaitGroup //it\"s possible to do the prep steps + free dlock before mem is actually finished, but it doesn\"t feel right...
	wg.Add(2)
		go func() { <- rt.parent.dlock ; wg.Done() }()
		go func() {	if rt.parent.mem != nil { <- rt.parent.memlock } ; wg.Done() }()
	wg.Wait()

	defer func() { 
		rt.parent.dlock <- true
		if rt.parent.mem != nil { rt.parent.memlock <- true }	
	}()

	//make a set of the old column names (used later)
	orgset := make(stringset, len(rt.cols))
	for _, orgrc := range rt.cols { orgset[orgrc.name] = empty{} }

	//validate the new columns, make a set of them, and prepare them for Rtable creation
	ncols := make([]rcol, len(newcols) +  rt.ddint)
	coldefs := make([]string, len(newcols) +  rt.ddint)
	var pkdefs []string
	var i int = rt.ddint
	newset := make(stringset, len(newcols) + rt.ddint)
	if rt.dd { ncols[0] = orgDdCol ; newset[orgDdCol.name] = empty{} ; coldefs[0] = "\"" + orgDdCol.name + "\" " + orgDdCol.ext }
	
	for _, newc := range newcols { 
		if (newc.Name == "") { return ErrInvalidTable }
		//I should probably check if Col.Ext is valid, buuut...https://www.sqlite.org/syntax/column-constraint.html

		if newset.has(newc.Name) { return ErrInvalidTable }
		newset[newc.Name] = empty{} 

		ncols[i] = rcol{name: newc.Name, ext: newc.Ext, pk: newc.Pk}
		coldefs[i] = "\"" + newc.Name + "\" " + newc.Ext
		if newc.Pk { pkdefs = append(pkdefs, "\"" + newc.Name + "\"")}

		i++
	}

	sporgnames := make([]string, len(remap))
	spnewnames := make([]string, len(remap))
	i = 0
	for orgcname, newcname := range remap {
		if !orgset.has(orgcname) || !newset.has(newcname) { return ErrIsNotPresent }
		
		sporgnames[i] = "\"" + orgcname + "\""
		spnewnames[i] = "\"" + newcname + "\""
		i++
	}

	var tempcreate string
	if len(pkdefs) == 0 {
		tempcreate = "CREATE TEMPORARY TABLE \"" + rt.name + "\"(" + strings.Join(coldefs, ", ") + ");"
	} else {
		tempcreate = "CREATE TEMPORARY TABLE \"" + rt.name + "\"(" + strings.Join(coldefs, ", ") + ", PRIMARY KEY(" + strings.Join(pkdefs, ", ") + "));"
	}

	//TODO: add specifiers of into which columns to insert the values
	var orgtransfer string = "INSERT INTO \"temp\".\"" + rt.name + "\"(" + strings.Join(spnewnames, ", ") + ") SELECT " + strings.Join(sporgnames, ", ") + " FROM \"main\".\"" + rt.name + "\";"

	normcreate := "CREATE TABLE \"" + rt.name + "\" AS SELECT * FROM \"temp\".\"" + rt.name + "\";"
	

	edit_func := func(db *sqlx.DB, errout chan error) { defer wg.Done()
		if db == nil { return }

		tx, err := db.Beginx() //begin transaction
		if err != nil { errout <- err ; return }

			_, err = tx.Exec(tempcreate) //create temp table
			if err != nil { tx.Rollback(); errout <- err ; return }

			_, err = tx.Exec(orgtransfer) //move + reformat/retarget from old to temp
			if err != nil { tx.Rollback(); errout <- err ; return }

			_, err = tx.Exec("DROP TABLE \"main\".\"" + rt.name + "\";") //drop old table
			if err != nil { tx.Rollback(); errout <- err ; return }

			_, err = tx.Exec(normcreate) //create new table with data of temp
			if err != nil { tx.Rollback(); errout <- err ; return }

			_, err = tx.Exec("DROP TABLE \"temp\".\"" + rt.name + "\";") //drop temp
			if err != nil { tx.Rollback(); errout <- err ; return }

		err = tx.Commit() //end transaction
		if err != nil { errout <- err ; return }
	}

	errin := make(chan error, 2)
	wg.Add(2) //edit both memory and disk at the same time
		go edit_func(rt.parent.db, errin)
		go edit_func(rt.parent.mem, errin)
	wg.Wait()
	if len(errin) != 0 { return <- errin }
	
	//in this order to make sure anything that takes over next has the right data
	rt.cols = ncols
	rt.parent.logEvent(log_editT, rt.name, "")

	return nil
}




//tries to return the true number of rows (including dd'd) in <rt> (err -> -1) 
func (rt *Rtable)Count() (num int) {
	if !rt.valid() { return -1 }

	<- rt.parent.dlock
	defer func() { rt.parent.dlock <- true }()

	err := rt.parent.db.Get(&num, "SELECT COUNT(*) FROM \"main\".\"" + rt.name + "\";")
	if err != nil { return -1 }

	return num
}

//tries to return the true number of rows (including dd'd) in <rt>\"s in-memory representation (err -> -1) 
func (rt *Rtable)CountMem() (num int) {
	if !rt.valid() { return -1 }
	if rt.parent.mem == nil { return -1 }

	<- rt.parent.memlock
	defer func() { rt.parent.memlock <- true }()

	err := rt.parent.mem.Get(&num, "SELECT COUNT(*) FROM \"main\".\"" + rt.name + "\";")
	if err != nil { return -1 }

	return num
}




//tries to interpret <data> as {x} rows of <rt>, then insert (or <cbh>) it into <rt>
func (rt *Rtable)InsertData(cbh conflict_behaviour, data... any) (err error) {
	if !rt.valid() { return ErrInvalidTable }

	<- rt.parent.dlock
	defer func() { rt.parent.dlock <- true }()

	if (len(data) == 0) { return nil }

	if (len(data) % (len(rt.cols) - rt.ddint)) != 0 { return ErrBadData }
	rowcount := (len(data) / (len(rt.cols) - rt.ddint))

	rowval_ph := "(?"
	for i := 1; i < (len(rt.cols) - rt.ddint); i++ { rowval_ph += ", ?" }
	rowval_ph += ")"

	allval_ph := rowval_ph
	for i := 1; i < rowcount; i++ { allval_ph += "," + rowval_ph } 

	colidef := "(\"" + rt.cols[rt.ddint].name + "\""
	for _, rcol := range rt.cols[rt.ddint + 1:] {
		colidef += ",\"" + rcol.name + "\""
	}
	colidef += ")"

	_, err = rt.parent.db.Exec("INSERT OR " + string(cbh) + " INTO \"main\".\"" + rt.name + "\" " + colidef + " VALUES " + allval_ph + ";", data...)
	if err != nil { return err }

	return nil
}

//tries to interpret <data> as {x} rows of <rt>, then insert (or <cbh>) it into <rt>\"s in-memory version
func (rt *Rtable)InsertMemData(cbh conflict_behaviour, data... any) (err error) {
	if !rt.valid() { return ErrInvalidTable }
	if rt.parent.mem == nil { return ErrNoMem }

	<- rt.parent.memlock
	defer func() { rt.parent.memlock <- true }()

	if (len(data) == 0) { return nil }

	if (len(data) % (len(rt.cols) - rt.ddint)) != 0 { return ErrBadData }
	rowcount := (len(data) / (len(rt.cols) - rt.ddint))

	rowval_ph := "(?"
	for i := 1; i < len(rt.cols); i++ { rowval_ph += ", ?" }
	rowval_ph += ")"

	allval_ph := rowval_ph
	for i := 1; i < rowcount; i++ { allval_ph += "," + rowval_ph } 

	colidef := "(\"" + rt.cols[rt.ddint].name + "\""
	for _, rcol := range rt.cols[rt.ddint + 1:] {
		colidef += ",\"" + rcol.name + "\""
	}
	colidef += ")"

	_, err = rt.parent.mem.Exec("INSERT OR " + string(cbh) + " INTO \"main\".\"" + rt.name + "\" " + colidef + " VALUES " + allval_ph + ";", data...)
	if err != nil { return err }

	return nil
}




//extracts <condarr> into an sql-compatible string, and an array of values to replace placeholders
func (condarr Condarr)clausify() (res string, subs []any) {
	if len(condarr) == 0 { return "", []any{} }
	
	lrelmap := map[bool]string{true: " AND ", false: " OR "}
	subs = make([]any, len(condarr))

	res += "(\"" + condarr[0].Cname + "\" " + string(condarr[0].Op) + " ? )"
	subs[0] = condarr[0].Val

	i := 1
	for _, cond := range condarr[1:] {
		if cond.Ljoin { res = "(" + res + ") " } //encapsulate preceding conditions

		res += lrelmap[cond.Lrel]

		res += "(\"" + cond.Cname + "\" " + string(cond.Op) + " ? )"
		subs[i] = cond.Val

		i++
	}
	res = " WHERE " + res

	return res, subs
} 

//extracts <ordarr> into an sql-compatible string
func (ordarr Ordarr)clausify() (res string) {
	if len(ordarr) == 0 { return "" }

	dirmap := map[bool]string{true: " ASC", false: " DESC"}
	nwhmap := map[bool]string{true: " NULLS FIRST", false: " NULLS LAST"}

	res += " ORDER BY \"" + ordarr[0].Cname + "\"" + dirmap[ordarr[0].Dir] + nwhmap[ordarr[0].Nullwh]
	for _, ord := range ordarr[1:] {
		res += ", \"" + ord.Cname + "\"" + dirmap[ord.Dir] + nwhmap[ord.Nullwh]
	}
	res += " "
	return res
}

/*
tries to return a slice of slices, which represent rows of <rt> (on-disk)
 -	 negative <index> indexes from the end of <rt>, instead of the beginning (-1 -> last item)
 -	 negative <count> means how many rows of <rt> to leave out of the selection (-1 -> leave out 0 rows)
 -	 <condarr> specifies conditions which must be true for a row to be retrieved
 -	 <ordarr> specifies the order of retrieval
*/
func (rt *Rtable)GetData(index int, count int, condarr Condarr, ordarr Ordarr) (data [][]any, err error){
	if !rt.valid() { return [][]any{}, ErrInvalidTable }

	<- rt.parent.dlock
	defer func() { rt.parent.dlock <- true }()

	var perlen int //perceived length of the table
	if (index < 0) || (count < 0) { 
		err = rt.parent.db.Get(&perlen, "SELECT COUNT(*) FROM \"main\".\"" + rt.name + "\";")
		if err != nil { return [][]any{}, err }
		perlen++ //because the lowest neg value of (index, count) is -1, and that should select everything
	}
	offset := perlen * (index >> (strconv.IntSize -1)) * -1 + index //pos -> index ; neg -> len(rt) - index
	limit := perlen * (count >> (strconv.IntSize -1)) * -1 + count //pos -> count ; neg -> len(rt) - count

	if rt.dd { condarr = append(condarr, orgDdColIs0) } //do not include "deleted" rows
	where, wheresubs := condarr.clausify()
	order_by := ordarr.clausify()

	rows, err := rt.parent.db.Queryx("SELECT * FROM \"main\".\"" + rt.name + "\"" + where + order_by + "LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset) + ";", wheresubs...)
	if err != nil { return [][]any{}, err }
	defer rows.Close()

	for rows.Next() {
		row_vals, err := rows.SliceScan()
		if err != nil { return data, err }

		data = append(data, row_vals[rt.ddint:]) //strip the dd col
	}

	return data, nil
}

/*
tries to return a slice of slices, which represent rows of <rt> (in-memory)
 -	 negative <index> indexes from the end of <rt>, instead of the beginning (-1 -> last item)
 -	 negative <count> means how many rows of <rt> to leave out of the selection (-1 -> leave out 0 rows)
 -	 <condarr> specifies conditions which must be true for a row to be retrieved
 -	 <ordarr> specifies the order of retrieval
*/
func (rt *Rtable)GetMemData(index int, count int, condarr Condarr, ordarr Ordarr) (data [][]any, err error){
	if !rt.valid() { return [][]any{}, ErrInvalidTable }
	if rt.parent.mem == nil { return [][]any{}, ErrNoMem }

	<- rt.parent.memlock
	defer func() { rt.parent.memlock <- true }()

	var perlen int //perceived length of the table
	if (index < 0) || (count < 0) { 
		err = rt.parent.db.Get(&perlen, "SELECT COUNT(*) FROM \"main\".\"" + rt.name + "\";")
		if err != nil { return [][]any{}, err }
		perlen++ //because the lowest neg value of (index, count) is -1, and that should select everything
	}
	offset := perlen + index //pos -> index ; neg -> len(rt) - index
	limit := perlen + count //pos -> count ; neg -> len(rt) - count

	if rt.dd { condarr = append(condarr, orgDdColIs0) } //do not include "deleted" rows
	where, wheresubs := condarr.clausify()
	order_by := ordarr.clausify()

	rows, err := rt.parent.mem.Queryx("SELECT * FROM \"main\".\"" + rt.name + "\"" + where + order_by + "LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset) + ";", wheresubs...)
	if err != nil { return [][]any{}, err }
	defer rows.Close()

	for rows.Next() {
		row_vals, err := rows.SliceScan()
		if err != nil { return data, err }

		data = append(data, row_vals[rt.ddint:]) //strip the dd col
	}

	return data, nil
}

/*
tries to load (copy) <rt>\"s items from disk into memory 
 -	 negative <index> indexes from the end of <rt>, instead of the beginning (-1 -> last item)
 -	 negative <count> means how many rows of <rt> to leave out of the selection (-1 -> leave out 0 rows)
 -	 <cbh> is what to do when the copying encounters two rows which cannot exist at once in one table
 -	 <condarr> specifies conditions which must be true for a row to be retrieved
*/
func (rt *Rtable)LoadIntoMem(index int, count int, cbh conflict_behaviour, condarr Condarr) (err error){
	if !rt.valid() { return ErrInvalidTable }
	if rt.parent.mem == nil { return ErrNoMem }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- rt.parent.dlock ; wg.Done() }()
		go func() {	<- rt.parent.memlock ; wg.Done() }()
	wg.Wait()

	defer func() { 
		rt.parent.dlock <- true
		rt.parent.memlock <- true 	
	}()

	var perlen int //perceived length of the table
	if (index < 0) || (count < 0) { 
		err = rt.parent.db.Get(&perlen, "SELECT COUNT(*) FROM \"main\".\"" + rt.name + "\";")
		if err != nil { return err }
		perlen++ //because the lowest neg value of (index, count) is -1, and that should select everything
	}
	offset := perlen + index //pos -> index ; neg -> len(rt) - index
	limit := perlen + count //pos -> count ; neg -> len(rt) - count

	where, wheresubs := condarr.clausify()

	_, err = rt.parent.mem.Exec("ATTACH DATABASE \"" + rt.parent.path + "\" AS \"disk\";")
	if err != nil { return err }
	defer rt.parent.mem.Exec("DETACH DATABASE \"disk\";")

	_, err = rt.parent.mem.Exec("INSERT OR " + string(cbh) + "INTO \"main\".\"" + rt.name + "\" SELECT * FROM \"disk\".\"" + rt.name + "\"" + where + "LIMIT " + strconv.Itoa(limit) + " OFFSET " + strconv.Itoa(offset) + ";", wheresubs...)
	if err != nil { return err }

	return nil
}

//tries to irreversibly remove rows where <condarr> is true from <rt>\"s memory
func (rt *Rtable)UnloadFromMem(condarr Condarr) (err error){
	if !rt.valid() { return ErrInvalidTable }
	if rt.parent.mem == nil { return ErrNoMem }

	<- rt.parent.memlock
	defer func() { rt.parent.memlock <- true }()

	where, wheresubs := condarr.clausify()

	_, err = rt.parent.mem.Exec("DELETE FROM \"main\".\"" + rt.name + "\"" + where + ";", wheresubs...)
	if err != nil { return err }

	return nil
}

//tries to, depending on rt.dd, permanently delete or mark as outdated, rows from where <condarr> is true, both from <rt>\"s memory and disk
func (rt *Rtable)DeleteData(condarr Condarr) (err error){
	if !rt.valid() { return ErrInvalidTable }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- rt.parent.dlock ; wg.Done() }()
		go func() {	if rt.parent.mem != nil { <- rt.parent.memlock } ; wg.Done() }()
	wg.Wait()

	defer func() { 
		rt.parent.dlock <- true
		if rt.parent.mem != nil { rt.parent.memlock <- true } 	
	}()

	

	var delfunc func(db *sqlx.DB, errout chan error)
	if rt.dd {
		delfunc = func(db *sqlx.DB, errout chan error){ defer wg.Done()
			if db == nil { return }

			condarr = append(condarr, Condition{Ljoin: true, Lrel: false, Cname: orgDdCol.name, Op: Op_more, Val: 0}) //also increment any rows where the dd col is >= 0 
			where, wheresubs := condarr.clausify()

			_, err := db.Exec("UPDATE \"main\".\"" + rt.name + "\" SET \"" + orgDdCol.name + "\" = 1"/*(\"" + orgDdCol.name + "\" + 1)"*/ + where + ";", wheresubs...)
			if err != nil { errout <- err ; return }
		}

	} else {
		delfunc = func(db *sqlx.DB, errout chan error){ defer wg.Done()
			if db == nil { return } 

			where, wheresubs := condarr.clausify()
			_, err := db.Exec("DELETE FROM \"main\".\"" + rt.name + "\"" + where + ";", wheresubs...)
			if err != nil { errout <- err ; return } 
		}
	}

	errin := make(chan error, 2)
	wg.Add(2)
		go delfunc(rt.parent.db, errin)
		go delfunc(rt.parent.mem, errin)
	wg.Wait()
	if len(errin) != 0 { return <- errin }

	return nil
}

/*
tries to unmark the deletion of rows marked in the <ver>th last deletion (both memory and disk)
 -	 will not do anything if not rt.dd
 -	 will not change the <ver> required to address any other deletions
 -	 addressing a nonexistent deletion with <ver> will simply not affect any rows
 -	 <ver> is indexed starting at 1
 -	 negative <ver> will instead delete the <ver>th deletion
*/
func (rt *Rtable)UndoDelete(ver int) (err error) {
	if !rt.valid() { return ErrInvalidTable }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- rt.parent.dlock ; wg.Done() }()
		go func() {	if rt.parent.mem != nil { <- rt.parent.memlock } ; wg.Done() }()
	wg.Wait()

	defer func() { 
		rt.parent.dlock <- true
		if rt.parent.mem != nil { rt.parent.memlock <- true } 	
	}()

	if !rt.dd { return nil }

	if ver < 0 {
		var maxver int
		err := rt.parent.db.Get(&maxver, "SELECT MAX(\"" + orgDdCol.name + "\") FROM \"main\".\"" + rt.name + "\";")
		if (err != sql.ErrNoRows) && (err != nil) { return err }

		ver = maxver + ver //ver is negative -> negative indexing
	}

	undelfunc := func(db *sqlx.DB, errout chan error) { defer wg.Done()
		if db == nil { return }

		_, err := db.Exec("UPDATE \"main\".\"" + rt.name + "\" SET \"" + orgDdCol.name + "\" = 0 WHERE \"" + orgDdCol.name + "\" = ?", ver)
		if err != nil { errout <- err ; return }
	}

	errin := make(chan error, 2)
	wg.Add(2)
		go undelfunc(rt.parent.db, errin)
		go undelfunc(rt.parent.mem, errin)
	wg.Wait()
	if len(errin) != 0 { return <- errin }

	return nil
}

/*
tries to irreversibly remove rows marked in the <ver>th last deletion (both memory and disk)
 -	 will not do anything if not rt.dd
 -	 will not change the <ver> required to address any other deletions
 -	 addressing a nonexistent deletion with <ver> will simply not affect any rows
 -	 <ver> is indexed starting at 1
 -	 negative <ver> will instead delete the <ver>th deletion
*/
func (rt *Rtable)ConfirmDelete(ver int) (err error) {
	if !rt.valid() { return ErrInvalidTable }

	var wg sync.WaitGroup
	wg.Add(2)
		go func() { <- rt.parent.dlock ; wg.Done() }()
		go func() {	if rt.parent.mem != nil { <- rt.parent.memlock } ; wg.Done() }()
	wg.Wait()

	defer func() { 
		rt.parent.dlock <- true
		rt.parent.memlock <- true 	
	}()

	if !rt.dd { return nil }

	if ver < 0 {
		var maxver int
		err := rt.parent.db.Get(&maxver, "SELECT MAX(\"" + orgDdCol.name + "\") FROM \"main\".\"" + rt.name + "\";")
		if (err != sql.ErrNoRows) && (err != nil) { return err }

		ver = maxver + ver //ver is negative -> negative indexing
	}

	remfunc := func(db *sqlx.DB, errout chan error) { defer wg.Done()
		if db == nil { return }

		_, err := db.Exec("DELETE FROM \"main\".\"" + rt.name + "\" WHERE \"" + orgDdCol.name + "\" = ?", ver)
		if err != nil { errout <- err ; return }
	}

	errin := make(chan error, 2)
	wg.Add(2)
		go remfunc(rt.parent.db, errin)
		go remfunc(rt.parent.mem, errin)
	wg.Wait()
	if len(errin) != 0 { return <- errin }

	return nil
}

/*TODO:
- Add export to CSV
- Add export to checksum format or something (and integrate it)*/