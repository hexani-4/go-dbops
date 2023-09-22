package dbops

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

/*major changes:
- can now not have in-memory
- dbToStructureCompat now outputs a structure target map (done)
- change the entirety of loadIntoMemory and stuff around that (because that shouldn't be an exported function now) (done)
    - adapt to the new data_sources

- make error when a piece of desired structure is replaced by required one
	- add adding of required structure to ExtendDataSource

*/
var ErrUnknownSourceName error = errors.New("UnknownSourcePath error (dbops) - Check if this path/alias was added using AddDataSource()")
var ErrIsNotDatabase error = errors.New("ErrIsNotDatabase error (dbops) - The supplied filepath/file load did not end in .db, check for typo in extension")
var ErrDatabaseMerge error = errors.New("ErrDatabaseMerge error (dbops) - The supplied databases were not compatible, consider allowing skip_incompatible_tables?")
var ErrInvalidTable error = errors.New("ErrInvalidTable error (dbops) - The supplied param was not '<alias>.<tablename>' or '<tablename>'")
var ErrUnknownTableName error = errors.New("ErrUnknownTableName error (dbops) - The target database does not have a table of this name")
var ErrInvalidData error = errors.New("ErrInvalidData error (dbops) - The supplied data is not compatible with its target table, check the number of columns")
var ErrIsNotStruct error = errors.New("ErrIsNotStruct error (dbops) - The supplied param is not of type struct, and cannot accept data from a table")
var ErrIsReserved error = errors.New("ErrIsReserved error (dbops) - Part of the loaded/requested structure is needed by dbops, rename it - check for a 'DeleteState' column")
var ErrIsNotAllowedChar error = errors.New("ErrIsNotAllowedChar error (dbops) - The supplied DataSource alias contained the '_' character")
var ErrFatalSaveException error = errors.New("ErrFatalSaveException error (dbops) - Failed to save memory to disk due to internal data error...this shouldn't be possible")

type Table_value struct {
	Column string //name of the column (inside your database)
	Values []any //will be interpreted as Column = Values[0] OR Column = Values[1] ...
}

type Db_table struct {
	Columns      []string //formatted as []string{"name TEXT", "age INTEGER"}
	Primary_key []string //formatted as []string{"age, name"}
}

type Db_structure map[string]*Db_table

type deleteHistItem struct {
	table string
	to_what bool
	values []Table_value
}
var deleteHistory []*deleteHistItem

var Mem *sqlx.DB

var reserved_structure = Db_structure{"database_log": &Db_table{[]string{"key TEXT", "value TEXT"}, []string{"key"}}}
var reserved_columns = []string{"DeleteState INTEGER DEFAULT 0"} // these are also forced in reserved structures

type DataSource struct{
	Path string //May be used as a "name". Outdated versions will be <filename>_<unix_millis> (<DD-MM-YYYY>).db
	Alias string //May be used as a "name". 
	Tablenames []string //Exists because of optimalization reasons (lookup inside of FormatUInputTable())
}
var data_sources []DataSource


//Expects a DataSource's alias || path. Returns the DataSource's path
func NameToPath(name string) (string, error) {
	for _, source := range data_sources{
		if (name == source.Path) || (name == source.Alias) { return source.Path, nil }
	}
	return "", ErrUnknownSourceName
}

//Expects a DataSource's alias || path. Returns the DataSource's alias
func NameToAlias(name string) (string, error) {
	for _, source := range data_sources{
		if (name == source.Path) || (name == source.Alias) { return source.Alias, nil }
	}
	return "", ErrUnknownSourceName
}

//Returns [ name (= DataSource.Path || .Alias), tablename, dbops.ErrInvalidTable || nil ] from the input string, only validates that there was just 0 or 1 dot. 
//Here to save me the hassle of writing it out each time, use it if you like. :/ 
func FormatUInputTable(table string) (string, string, error) {
	split_table := strings.Split(table, ".")
	if len(split_table) >= 2 { 
		return "", "", ErrInvalidTable 

	} else if len(split_table) == 2 {
		return split_table[0], split_table[1], nil
	}

	for _, source := range data_sources {
		for _, tablename := range source.Tablenames {
			if split_table[0] == tablename {
				return source.Path, tablename, nil
			}
		}
	}

	return "", split_table[0], ErrUnknownTableName
}

func (structure Db_structure) tableCreateStatements(if_not_exists bool) []string {
	statement_list := make([]string, len(structure))
	i := 0
	for tablename, table := range structure {
		statement := "CREATE TABLE "
		if if_not_exists {
			statement += "IF NOT EXISTS "
		}
		statement += tablename
		statement += "(" + strings.Join(table.Columns, ", ")
		if len(table.Primary_key) != 0 {
		statement += ", PRIMARY KEY("
		statement += strings.Join(table.Primary_key, ", ") + ")"
		}
		statement += ");"

		statement_list[i] = statement
		i++
	}
	return statement_list
}

func structureFromDb(db *sqlx.DB) (Db_structure, error) {
	type column_info struct{
		index int
		name string
		datatype string
		allows_null bool
		default_value sql.NullString
		default_string string
		pk bool
	}
	var structure Db_structure = make(Db_structure)

	var table_list []string
	if err := db.Select(&table_list, "SELECT name FROM sqlite_master WHERE type='table';"); err != nil {
		return structure, err
	}
	
	for _, tablename := range table_list {
		var table_structure Db_table

		cols, err := db.Queryx(fmt.Sprintf("PRAGMA table_info('%s');", tablename))
		if err != nil {return structure, err}

		for cols.Next() {
			var column column_info
		
			err := cols.Scan(&column.index, &column.name, &column.datatype, &column.allows_null, &column.default_value, &column.pk)
			if err != nil {log.Fatalln(err)}
			column.default_string = column.default_value.String

			table_structure.Columns = append(table_structure.Columns, column.name + " " + column.datatype)
			if (column.pk){ table_structure.Primary_key = append(table_structure.Primary_key, column.name)}
			}
		structure[tablename] = &table_structure
		cols.Close()
	}
	return structure, nil
}

func filepathFromDb(db *sqlx.DB) string {
	type database_list_info struct{
		seq string
		name string
		file string
	}
	var info database_list_info
	
	rows, err := db.Queryx("PRAGMA database_list;")
	if err != nil {log.Fatalln(err)}

	rows.Next()
	rows.Scan(&info.seq, &info.name, &info.file)
	rows.Close()
	
	return info.file
}

type compare_type_map map[string][]string
func compareTypeFromDb(db *sqlx.DB) (compare_type_map, error) {
	type column_info struct{
		index int
		name string
		datatype string
		allows_null bool
		default_value sql.NullString
		default_string string
		pk bool
	}
	var structure compare_type_map = make(compare_type_map)

	var table_list []string
	err := db.Select(&table_list, "SELECT name FROM sqlite_master WHERE type='table';")
	if err != nil {return structure, err}

	for _, tablename := range table_list {
		cols, err := db.Queryx(fmt.Sprintf("PRAGMA table_info('%s');", tablename))
		if err != nil {return structure, err}


		var type_list []string

		for cols.Next() {
			var column column_info
		
			err := cols.Scan(&column.index, &column.name, &column.datatype, &column.allows_null, &column.default_value, &column.pk)
			if err != nil {return structure, err}
			column.default_string = column.default_value.String

			type_list = append(type_list, column.datatype)
		}
		structure[tablename] = type_list
	}
	return structure, nil
}

type db_target_map map[*sqlx.DB]map[*sqlx.DB]map[string]string 
func dbToDbCompat(db_list []*sqlx.DB, ignore_incompatibility bool) (db_target_map, bool, error){ 
	var db_target_map db_target_map = make(db_target_map)

	one_or_zero_edge_case := 1
	if len(db_list) == 1{
		one_or_zero_edge_case = 0
	}

	for i, target_db := range db_list[: len(db_list) - one_or_zero_edge_case]{
		if db_target_map[target_db] == nil { db_target_map[target_db] = make(map[*sqlx.DB]map[string]string) }
		
		target, err := compareTypeFromDb(target_db)
		if err != nil {return nil, false, err}
		
		for _, source_db := range db_list[i:]{
			if target_db == source_db { continue }

			if db_target_map[target_db][source_db] == nil { db_target_map[target_db][source_db] = make(map[string]string) }

			source, err := compareTypeFromDb(source_db)
			if err != nil {return nil, false, err}

			t_table_iterator:
			for t_name, t_types := range target {

				u := 0
				equivalent_map := make([]string, len(source)) 
				for s_name, s_types := range source {
					if len(t_types) == len(s_types){
						
						if reflect.DeepEqual(t_types, s_types) {

							if s_name == t_name {
								fmt.Println("database compatibility -- ", target_db, t_name, " shares structure and name with ", source_db, s_name, ", declaring relation")
								db_target_map[target_db][source_db][t_name] = s_name
								continue t_table_iterator
							}
							fmt.Println("database compatibility -- ", target_db, t_name, " shares structure with ", source_db, s_name, ", may declare relation")

							equivalent_map[u] = t_name
						
						}
					}

				u++}

				for _, s_name := range equivalent_map{
					if s_name != "" {
						db_target_map[target_db][source_db][t_name] = s_name
						continue t_table_iterator
					}
				}	

				if !ignore_incompatibility { 
					fmt.Println("    -- compatibility check failed")
					return db_target_map, false, nil
				} 
			}

		}
	}
	//add cleanup of maps somehow? delete if encounter default value
	//will only be a problem if ignore_incompatibility = true

	fmt.Println("    -- compatibility check succeeded")
	return db_target_map, true, nil
}

type structure_target_map map[string]string
func dbToStructureCompat(db *sqlx.DB, structure Db_structure, ignore_incompatibility bool) (structure_target_map, bool, error) {
	var structure_target_map structure_target_map = make(structure_target_map)

	var target compare_type_map = make(compare_type_map)
	for tablename, table := range structure{
		type_list := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			type_list[i] = strings.Split(col, " ")[1]
		}
		target[tablename] = type_list
	}

	source, err := compareTypeFromDb(db)
	if err != nil {return nil, false, err}


	t_table_iterator:
	for t_name, t_types := range target {
		u := 0
		match_array := make([]string, len(source)) 
		for s_name, s_types := range source {
			if len(t_types) == len(s_types){
				
				if reflect.DeepEqual(t_types, s_types) {

					if s_name == t_name {
						fmt.Println("database to structure compatibility -- ", db, t_name, " shares structure and name with ", s_name, ", declaring relation")
						structure_target_map[t_name] = s_name
						continue t_table_iterator
					}
					fmt.Println("database to structure compatibility -- ", db, t_name, " shares structure with ", s_name, ", may declare relation")

					match_array[u] = t_name
				
				}
			}

		u++}

		for _, s_name := range match_array{
			if s_name != "" {
				structure_target_map[t_name] = s_name
				continue t_table_iterator
			}
		}	

		if !ignore_incompatibility { 
			fmt.Println("    -- compatibility check failed")
			return structure_target_map, false, nil
		} 
	}

	return structure_target_map, true, nil
}

//returned bool signifies if <main> was attached as <alias>, instead of <other> being attached as <alias>. 
func attachDatabases(main *sqlx.DB, other *sqlx.DB, alias string) (*sqlx.DB, bool, error){
	main_path := filepathFromDb(main)
	_, main_name := filepath.Split(main_path)

	other_path := filepathFromDb(other)
	_, other_name := filepath.Split(other_path)

    if (other_name != "") {
		_, err := main.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS %s;", other_path, alias))
		return main, false, err

	} else if (other_name == "" && main_name != "") {
		_, err := other.Exec(fmt.Sprintf("ATTACH DATABASE '%s' AS %s;", main_path, alias))
		return other, true, err
	
	} else { return nil, false, errors.New("attachDatabases() - attempted to attach two in-memory databases")}
}

//layers = (a, b, c, d), merge direction: b[d] <- a[c]
func (target_map db_target_map) performMerge(bidirectional bool, condition_clause string) error{
	for target_db, bottom_db_map := range target_map{
		
		for source_db, table_map := range bottom_db_map{

			joined_db, do_swap, err := attachDatabases(target_db, source_db, "other")
			if err != nil {
				return err
			}
			
			var master string
			var slave string
			if !do_swap {
				master = "main"
				slave = "other"
			} else {
				master = "other"
				slave = "main"
			}

			for master_t, slave_t := range table_map{

				_, err := joined_db.Exec(fmt.Sprintf("INSERT OR IGNORE INTO %s.%s SELECT * FROM %s.%s %s;", slave, slave_t, master, master_t, condition_clause))
				if err != nil {
					joined_db.Exec("DETACH DATABASE other;")
					return err}

				if bidirectional {
				_, err := joined_db.Exec(fmt.Sprintf("INSERT OR IGNORE INTO %s.%s SELECT * FROM %s.%s %s;", master, master_t, slave, slave_t, condition_clause))
				if err != nil {
					joined_db.Exec("DETACH DATABASE other;")
					return err}
				}
			}

			_, err = joined_db.Exec("DETACH DATABASE other;")
			if err != nil { return err }
		}
	}

	return nil
}


//Inserts OR Ignores all rows from each table (of each db) into a table with the same num of columns && order of datatypes (column names don't matter) in all dbs. 
//Does not care about intermediate memory, will directly merge the files. 
func MergeSources(names []string, skip_incompatible_tables bool) error {
	var err error

	paths := make([]string, len(names))
	for i, name := range names {
		paths[i], err = NameToPath(name)
	}
	if err != nil { return err }

	dbs := make([]*sqlx.DB, len(paths))
	for i, path := range paths {
		dbs[i], err = sqlx.Connect("sqlite3", path)
		defer dbs[i].Close()
	}
	if err != nil { return err }


	db_to_db_target_map, success, err := dbToDbCompat(dbs, skip_incompatible_tables)
	if err != nil {
		return err
	} else if !success {
		return ErrDatabaseMerge
	}
	
	err = db_to_db_target_map.performMerge(true, "")
	return err
}

//Inserts OR Ignores all rows from each table (of each source db) into a table with the same num of columns && order of datatypes (column names don't matter) in the target db. 
//Does not care about intermediate memory, will directly merge the files. 
func MergeSourcesInto(source_names []string, target_name string, skip_incompatible_tables bool) error {

	t_path, err := NameToPath(target_name)
	
	paths := make([]string, len(source_names))
	for i, name := range source_names {
		paths[i], err = NameToPath(name)
	}
	if err != nil { return err }

	t_db, err := sqlx.Connect("sqlite3", t_path)
	if err != nil { return err }
	defer t_db.Close()

	dbs := make([]*sqlx.DB, len(paths))
	for i, path := range paths {
		dbs[i], err = sqlx.Connect("sqlite3", path)
		defer dbs[i].Close()
	}
	if err != nil { return err }


	db_to_db_target_map, success, err := dbToDbCompat(append([]*sqlx.DB{t_db}, dbs...), skip_incompatible_tables)
	if err != nil {
		return err
	} else if !success {
		return ErrDatabaseMerge
	}

	var altered_target_map db_target_map = make(map[*sqlx.DB]map[*sqlx.DB]map[string]string)

	top_db_iterator:
	for top_db, bottom_db_map := range db_to_db_target_map {

		if top_db == t_db { //may be completely broken, TODO: TEST
			for source_db, table_map := range bottom_db_map {
				altered_target_map[source_db] = make(map[*sqlx.DB]map[string]string)
				altered_target_map[source_db][t_db] = make(map[string]string)
				var inverted_table_map map[string]string = make(map[string]string)
				for m_table, s_table := range table_map{
					inverted_table_map[s_table] = m_table
				}
				altered_target_map[source_db][t_db] = inverted_table_map
			}


			continue top_db_iterator
		}

		for source_db, table_map := range bottom_db_map {
			if source_db == t_db {
				altered_target_map[t_db] =  make(map[*sqlx.DB]map[string]string)
				altered_target_map[t_db][source_db] = make(map[string]string)
				altered_target_map[t_db][source_db] = table_map
			}
		}
	}
	err = altered_target_map.performMerge(false, "")
	return err
}

//Returns the number of items that was actually loaded. 
//To be used for preloading data (to speed up access later) when using Mem != nil, returns (0, nil) if Mem != nil is disabled. 
//Examples of working order_clause(s): "ORDER BY col1, col2 ASC", "". 
//index < 0 -> index = height of memory. count < 0 -> count = height of table - index 
func LoadIntoMemory(table string, order_clause string, index int, count int) (int, error) {
	if Mem == nil { return 0, nil }

	name, tablename , err := FormatUInputTable(table)
	if err != nil { return 0, err }

	name_alias, _ := NameToAlias(name) //don't care about error since FormatUInputTable() is called before this
	if err != nil { return 0, err }

	mem_tablename := name_alias + "_" + tablename

	var old_mem_length int
	if err := Mem.Get(&old_mem_length, fmt.Sprintf("SELECT MAX(ROWID) FROM %s LIMIT 1;", mem_tablename)); err != nil {return 0, err}

	if index < 0 {
		index = old_mem_length
		fmt.Printf("loadIntoMemory() -- index < 0; interpreted as height of memory.%s (= %d )\n", mem_tablename, index)
	}

	name_path, _ := NameToPath(name) //don't care about error since FormatUInputTable() is called before this
	db, err := sqlx.Connect("sqlite3", name_path)
	if err != nil { return 0, err }
	defer db.Close()

	if count < 0 {
		var source_length int
		if err := db.Get(&source_length, fmt.Sprintf("SELECT MAX(ROWID) FROM %s LIMIT 1;", tablename)); err != nil {return 0, err}
		count = source_length - index
		fmt.Printf("loadIntoMemory() -- count < 0; interpreted as height of %s - index (= %d )\n", table, count)
	}


	joined_db, _, err := attachDatabases(Mem, db, "source")
	//don't care about swap since main is always in-memory
	if err != nil { return 0, err }
	defer joined_db.Exec("DETACH DATABASE source;")

	mem_structure, err := structureFromDb(Mem)
	if err != nil { return 0, err }
	mem_table_pk := strings.Join(mem_structure[mem_tablename].Primary_key, ", ")

	insert_statement := fmt.Sprintf("INSERT OR IGNORE INTO main.%s SELECT DISTICT %s FROM source.%s %s LIMIT %d OFFSET %d;", mem_tablename, mem_table_pk, tablename, order_clause, count, index)
	_, err = joined_db.Exec(insert_statement)
	if err != nil { return 0, err }

	var mem_length int
	err = Mem.Get(&mem_length, fmt.Sprintf("SELECT MAX(ROWID) FROM %s LIMIT 1;", mem_tablename))
	retrieved_count := mem_length - old_mem_length
	if err != nil { return retrieved_count, err }

	fmt.Println("loadIntoMemory() -- loaded ", retrieved_count, " / ", count, " items.")

	return retrieved_count, nil
}


//Creates the .db file, along with any non-existent directories along its path. The database will not have any schema. Does not add the <path> (aka database) to data_sources. 
//Setting overwrite_existing to true will make a new database even if the path already exists (would return os.ErrExist otherwise). 
func CreateDataSource(path string, overwrite_existing bool) error {
	if filepath.Ext(path) != ".db" {
		return ErrIsNotDatabase
	}

	dir_path, _ := filepath.Split(path)

	_, err := os.Stat(path)
	if os.IsNotExist(err) {

		err := os.MkdirAll(dir_path, 0700)
		if (err != nil){ return err }

		db, err := sqlx.Connect("sqlite3", path)
		if (err != nil){ return err }
		defer db.Close()

	} else if err != nil {
		return err

	} else if overwrite_existing {
		err := os.Remove(path)
		if err != nil { return err }

		db, err := sqlx.Connect("sqlite3", path)
		if (err != nil){ return err }
		defer db.Close()
	} else {
		return os.ErrExist
	}
	
	return nil
}

//Adds <path> to data_sources, making it (and its alias) usable in PopulateDataSource, OutdateDataSource, RetrieveData, etc.
//If Mem != nil is enabled, will also create all of the DataSource's tables to memory (as '<alias>_<tablename>')
func AddDataSource(path string, alias string) error {
	for _, char := range alias {
		if char == '_' {
			return ErrIsNotAllowedChar
		}
	}


	_, err := os.Stat(path)
	if err != nil { return err }

	if filepath.Ext(path) != ".db" {
		return ErrIsNotDatabase
	}
	
	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return err }
	defer db.Close()

	structure, err := structureFromDb(db)
	if err != nil { return err }

	for r_tablename, r_table := range reserved_structure {
		
		table, exists := structure[r_tablename]
		if exists {
			if !reflect.DeepEqual(table, r_table) {
				return ErrIsReserved
			}
		} else {
			structure[r_tablename] = r_table
		}
	}

	for _, table := range structure {
		for _, r_column := range reserved_columns{

			semiform_r_column := strings.FieldsFunc(r_column, quoteSplit)
			var form_r_column string
			if len(semiform_r_column) == 1 {
				form_r_column = strings.Fields(semiform_r_column[0])[0]
			} else {
				form_r_column = semiform_r_column[0]
			} 

			for _, column := range table.Columns{
				
				semiform_column := strings.FieldsFunc(column, quoteSplit)
				var form_column string
				if len(semiform_column) == 1 {
					form_column = strings.Fields(semiform_column[0])[0]
				} else {
					form_column = semiform_column[0]
				}
				
				if form_r_column == form_column { return ErrIsReserved } //would mean that a reserved column name was requested
			}
		}
	}

	
	tablenames := make([]string, len(structure))

	i := 0

	
	for tablename, table := range structure {
		tablenames[i] = tablename
		i++

		if Mem != nil {
			create_statement := "CREATE TABLE " + alias + "_" + tablename //modified .tableCreateStatements()
			create_statement += "(" + strings.Join(table.Columns, ", ")
			if len(table.Primary_key) != 0 {
				create_statement += ", PRIMARY KEY("
				create_statement += strings.Join(table.Primary_key, ", ") + ")"
			}
			create_statement += ");"

			_, err := Mem.Exec(create_statement)
			if err != nil { return err }
		}
	}

	data_sources = append(data_sources, DataSource{Path: path, Alias: alias, Tablenames: tablenames})
	return nil
}

//Deletes the underlying file of <name>, the checks for an error (from the os.Remove()), if no error occured, removes <name> from data_sources.
func DeleteDataSource(name string) error {
	path, err := NameToPath(name)
	if err != nil { return err }

	err = os.Remove(path)
	if err != nil { return err }
	
	RemoveDataSource(path) //don't care about error, because only ErrUnknownSourceName can be returned, which would already happen from NameToPath()
	return nil
}

//Removes <name> from data_sources, does not delete the underlying file. 
//If Memory is enabled, will DROP (!!) all in-memory tables originating from this DataSource. 
//If you wish to delete the underlying file, use DeleteDataSource().
func RemoveDataSource(name string) error {
	for i, source := range data_sources{
		if (name == source.Path) || (name == source.Alias) { 

			if Mem != nil {
				db, err := sqlx.Connect("sqlite3", source.Path)
				if err != nil { return err }
				defer db.Close()

				for _, tablename := range source.Tablenames {
					drop_statement := "DROP TABLE "
					drop_statement += source.Alias + "_" + tablename
					_, err := Mem.Exec(drop_statement)
					if err != nil { return err }
				} 
			}

			data_sources = append(data_sources[:i], data_sources[i + 1:]...)
			return nil
		 }
	}
	return ErrUnknownSourceName
}

func quoteSplit(r rune) bool {
	return r == '\'' || r == '"' 
}



//Attempts to add all of <structure> into <name>. Will return error (from sqlx) if that tablename already exists. 
//If Memory is enabled, will try to also add <structure> to it (as '<alias_from_name>_<tablename>'). 
func ExtendDataSource(name string, structure Db_structure) error {
	var ds_index int
	var path string
	var alias string
	if !func() bool { //modified NameToPath()
		for i, source := range data_sources {
			if source.Path == name || source.Alias == name {
				ds_index = i
				path = source.Path
				alias = source.Alias
				return true 
			} 
		}
		return false
	}() { return ErrUnknownSourceName }

	fmt.Println(structure)
	for r_tablename, r_table := range reserved_structure {
		_, exists := structure[r_tablename] 
		if exists { return ErrIsReserved } //would mean that a reserved tablename was requested

		structure[r_tablename] = r_table
	}
	fmt.Println(structure)

	tablenames := make([]string, len(structure))
	i := 0
	for tablename, table := range structure {
		for _, r_column := range reserved_columns{

			semiform_r_column := strings.FieldsFunc(r_column, quoteSplit)
			var form_r_column string
			if len(semiform_r_column) == 1 {
				form_r_column = strings.Fields(semiform_r_column[0])[0]
			} else {
				form_r_column = semiform_r_column[0]
			} 

			for _, column := range table.Columns{
				
				semiform_column := strings.FieldsFunc(column, quoteSplit)
				var form_column string
				if len(semiform_column) == 1 {
					form_column = strings.Fields(semiform_column[0])[0]
				} else {
					form_column = semiform_column[0]
				}
				
				if form_r_column == form_column { return ErrIsReserved } //would mean that a reserved column name was requested
			}
			
			table.Columns = append(table.Columns, r_column)
		}

		tablenames[i] = tablename
		i++
	}
	fmt.Println(structure)


	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return err }
	defer db.Close()
	
	for e, statement := range structure.tableCreateStatements(false){
		_, err := db.Exec(statement)
		if err != nil { return err }

		tablename, table := tablenames[e], structure[tablenames[e]]  //insert nil row
			nil_statement := "INSERT INTO " + tablename + " VALUES("
			for _, col := range table.Columns {
				switch strings.Split(col, " ")[1]{
				case "INTEGER":
					nil_statement += "0, "
				case "TEXT":
					nil_statement += "'none', "
				case "BLOB":
					nil_statement += "'none', "
				case "REAL":
					nil_statement += "0.0, "
				default: //may cause errors
					nil_statement += "'none', "
				}
			}
	
			nil_statement = nil_statement[:len(nil_statement) - 2] + ");"
			_, err = db.Exec(nil_statement)
			if (err != nil) { return err }

		if Mem != nil {
			create_statement := "CREATE TABLE " + alias + "_" + tablename //modified .tableCreateStatements()
			create_statement += "(" + strings.Join(table.Columns, ", ")
			if len(table.Primary_key) != 0 {
				create_statement += ", PRIMARY KEY("
				create_statement += strings.Join(table.Primary_key, ", ") + ")"
			}
			create_statement += ");"

			_, err := Mem.Exec(create_statement)
			if err != nil { return err }

			_, err = Mem.Exec(nil_statement)
			if err != nil { return err }
			}

		data_sources[ds_index].Tablenames = append(data_sources[ds_index].Tablenames, tablenames[e])
	}

	return nil
}

//Will move the underlying path of <name> to (after creating non-existing directories along its path) <new_path> (which is a full filepath). 
//You may use NameToPath() to get the current full filepath of <name>. 
//Moves the underlying file of <name>, the checks for an error (from the os.Rename()), if no error occured, changes <name>'s path in data_sources (alias will stay the same). 
func MoveDataSource(name string, new_path string, overwrite_existing bool) error {
	if filepath.Ext(new_path) != ".db" {
		return ErrIsNotDatabase
	}
	
	var ds_index int
	var old_path string
	if !func() bool { //modified NameToPath()
		for i, source := range data_sources {
			if source.Path == name || source.Alias == name {
				ds_index = i
				old_path = source.Path
				return true 
			} 
		}
		return false
	}() { return ErrUnknownSourceName }

	_, err := os.Stat(new_path)
	if !(overwrite_existing && os.IsNotExist(err)) {
		return os.ErrExist
	} else if err != nil { return err }

	new_dir_path, _ := filepath.Split(new_path)
	err = os.MkdirAll(new_dir_path, 0700)
	if err != nil{ return err }

	err = os.Rename(old_path, new_path)
	if err != nil{ return err }

	data_sources[ds_index].Path = new_path

	return nil
}

//Renames <name> to <old-filename>_<unix-millis> (<DD-MM-YYYY>).db (formatted with the time of outdating) and creates a new database, with the same schema as <name>, which <name> now points to.
func OutdateDataSource(name string) error {
	path, err := NameToPath(name)
	if err != nil { return err }

	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return err }
		structure, err := structureFromDb(db)
		if err != nil { return err }	
	err = db.Close()
	if err != nil { return err }

	abs_filepath, err := filepath.Abs(path)
	if err != nil { return err }

	outdated_path := abs_filepath[:len(abs_filepath) - 3]

	outdated_path += fmt.Sprintf("_%020d (%s).db", time.Now().UnixMilli(), time.Now().Format(time.DateOnly))

	err = os.Rename(abs_filepath, outdated_path)
	if err != nil { return err }


	db, err = sqlx.Connect("sqlite3", path)
	if err != nil { return err }
		ExtendDataSource(name, structure)
	err = db.Close()
	if err != nil { return err }

	return nil
}

//Attempts to Insert OR Ignore the values into <table> (= tablename or source_alias/path.tablename). 
//If Mem != nil is enabled, will insert into its corresponding table instead. 
func InsertData(table string, values... any) error {
	name, tablename, err := FormatUInputTable(table)
		if err != nil { return err }

	if Mem != nil {
		alias, _ := NameToAlias(name) //don't care about err, since FormatUInputTable would catch it (= ErrUnknownSourceName)
		tablename =  alias + "_" + tablename
	}

	insert_statement := fmt.Sprintf("INSERT OR IGNORE INTO %s VALUES (", tablename)

	qm_array := make([]string, len(values))
	for i := range qm_array{
		qm_array[i] = "?"
	}

	insert_statement += strings.Join(qm_array, ", ") + ");"
	
	
	if Mem != nil {
		_, err = Mem.Exec(insert_statement, values...)
		if err != nil { return err }	

	} else {
		path, err := NameToPath(name)
		if err != nil { return err }

		db, err := sqlx.Connect("sqlite3", path) 
		if err != nil { return err }
		defer db.Close()

		_, err = db.Exec(insert_statement, values...)
		if err != nil { return err }	
	}
	return nil
}

//Returns a slice of <row_receiver>s (= exported struct with fields (of proper type, all exported) for all columns of <table>). 
//Examples of working order_clause(s): "ORDER BY col1, col2 ASC", "". 
//index < 0 -> index = height of table - count (= retrieves <count> rows from the end). count < 0 -> count = height of table - index (= retrieves all items from <index> to end of table) 
//index < 0 && count < 0 will retrieve 0 rows.
func GetDataByIndex(table string, order_clause string, index int, count int, row_receiver any) ([]any, error) {
	var row_slice []any
	
	rr_type := reflect.TypeOf(row_receiver)
	if rr_type.Kind() != reflect.Struct { return row_slice, ErrIsNotStruct }

	name, tablename, err := FormatUInputTable(table)
	if err != nil { return row_slice, err }

	var db *sqlx.DB
	if Mem != nil {
		
		name_alias, _ := NameToAlias(name) //don't care about err since FormatUInputTable() is called before this
		tablename = name_alias + "_" + tablename
		db = Mem

	} else {

		path, _ := NameToPath(name) //don't care about err since FormatUInputTable() is called before this
		db, err = sqlx.Connect("sqlite3", path)
		if err != nil { return row_slice, err }
		defer db.Close()
	}

	if (index < 0) || (count < 0){
		var source_length int
		if err := db.Get(&source_length, fmt.Sprintf("SELECT MAX(ROWID) FROM mian.%s LIMIT 1;", tablename)); err != nil {return row_slice, err}

		if index < 0 {
			index = source_length - count
			fmt.Printf("loadIntoMemory() -- index < 0; interpreted as height of %s - count (= %d )\n", table, index)
		}

		if count < 0 {
			count = source_length - index
			fmt.Printf("loadIntoMemory() -- count < 0; interpreted as height of %s - index (= %d )\n", table, count)
		}
	}

	rows, err := db.Queryx(fmt.Sprintf("SELECT * FROM main.%s %s LIMIT %d OFFSET %d;", tablename, order_clause, count, index))
	if err != nil { return row_slice, err }
	defer rows.Close()
	
	for rows.Next() {
		receiver := reflect.New(rr_type).Elem()

		err := rows.StructScan(receiver.Addr().Interface())
		if err != nil { return row_slice, err }

		row_slice = append(row_slice, receiver.Interface())
	}

	return row_slice, nil
}

//DRY thing, too lazy to copy the thing each time
//returns unpacked_values (in correct order), the clause string ("WHERE x=? AND (y=? OR y=?)")
func makeConditionClause(in []Table_value) ([]any, string) {
	var unpacked_values []any
	condition_array := make([]string, len(in))
	for i, col := range in {
		or_subarray := make([]string, len(col.Values))
		for e, val := range col.Values {
			or_subarray[e] = col.Column + " = ?"
			unpacked_values = append(unpacked_values, val)
		}
		condition_array[i] = "(" + strings.Join(or_subarray, " OR ") + ")" 
	}
	return unpacked_values, "WHERE " + strings.Join(condition_array, " AND ")
}

//Returns a slice of <row_receiver>s (= exported struct with fields (of proper type, all exported) for all columns of <table>). 
//Examples of working order_clause(s): "ORDER BY col1, col2 ASC", "". 
func GetDataByValue(table string, order_clause string, values []Table_value, row_receiver any) ([]any, error) {
	var row_slice []any
	
	rr_type := reflect.TypeOf(row_receiver)
	if rr_type.Kind() != reflect.Struct { return row_slice, ErrIsNotStruct }

	name, tablename, err := FormatUInputTable(table)
	if err != nil { return row_slice, err }

	var db *sqlx.DB
	if Mem != nil {
		
		name_alias, _ := NameToAlias(name) //don't care about err since FormatUInputTable() is called before this
		tablename = name_alias + "_" + tablename
		db = Mem

	} else {

		path, _ := NameToPath(name) //don't care about err since FormatUInputTable() is called before this
		db, err = sqlx.Connect("sqlite3", path)
		if err != nil { return row_slice, err }
		defer db.Close()
	}

	unpacked_values, condition_clause := makeConditionClause(values)
	

	rows, err := db.Queryx(fmt.Sprintf("SELECT * FROM %s %s %s;", tablename, condition_clause, order_clause), unpacked_values...)
	if err != nil { return row_slice, err }
	defer rows.Close()
	
	for rows.Next() {
		receiver := reflect.New(rr_type).Elem()

		err := rows.StructScan(receiver.Addr().Interface())
		if err != nil { return row_slice, err }

		row_slice = append(row_slice, receiver.Interface())
	}
	return row_slice, nil
}


//Sets the 'DeleteState' column of <table> where <values> to <to_what> (as an integer). 
func SetDeleteByValue(table string, to_what bool, values []Table_value) error {
	name, tablename, err := FormatUInputTable(table)
	if err != nil { return err }

	var db *sqlx.DB
	if Mem != nil {
		
		name_alias, _ := NameToAlias(name) //don't care about err since FormatUInputTable() is called before this
		tablename = name_alias + "_" + tablename
		db = Mem

	} else {

		path, _ := NameToPath(name) //don't care about err since FormatUInputTable() is called before this
		db, err = sqlx.Connect("sqlite3", path)
		if err != nil { return err }
		defer db.Close()
	}

	var to_what_int uint8
	if to_what { to_what_int = 1 }

	unpacked_values, condition_clause := makeConditionClause(values)
	_, err = db.Exec(fmt.Sprintf("UPDATE main.%s SET DeleteState=%b %s", tablename, to_what_int, condition_clause), unpacked_values...)
	if err != nil { return err }

	deleteHistory = append(deleteHistory, &deleteHistItem{table: table, to_what: to_what, values: values})
	return nil
}

//Undoes the most recent not-yet-undone SetDeleteByValue()  (= there's a list of things to undo, when you call this, the last item in the list gets removed (and undone)) 
//If there is nothing to undo, silently returns nil (no error) 
func UndoDelete() error {
	if len(deleteHistory) > 0 {
		HistItem := deleteHistory[len(deleteHistory) - 1]

		err := SetDeleteByValue(HistItem.table, !HistItem.to_what /*! is important*/, HistItem.values)
		if err != nil { return err }

		deleteHistory = deleteHistory[:len(deleteHistory) - 2] // - 2 because it has to also remove the new HistItem made by SetDeleteByValue()
	}
	return nil
}

//Removes all items where DeleteState=1 from <name> 
func RemoveDeleted(name string) error {
	path, err := NameToPath(name)
	if err != nil { return err }

	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return err }
	defer db.Close()

	structure, err := structureFromDb(db)
	if err != nil { return err }

	for tablename := range structure {
		_, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE DeleteState=1", tablename))
		if err != nil { return err }

		if Mem != nil {
			alias, _ := NameToAlias(name)
			_, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE DeleteState=1", alias + "_" + tablename))
			if err != nil { return err }
		}
	}
	return nil
}

//Removes all items where DeleteState=1 from <name> . 
//A variant of RemoveDeleted(), which only removes from a single table.  
func RemoveDeletedFromTable(table string) error {
	name, tablename, err := FormatUInputTable(table)
	if err != nil { return err }

	path, _ := NameToPath(name)
	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return err }
	defer db.Close()
	
	_, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE DeleteState=1", tablename))
	if err != nil { return err }

	if Mem != nil {
		alias, _ := NameToAlias(name)
		_, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE DeleteState=1", alias + "_" + tablename))
		if err != nil { return err }
	}
	return nil
}

//Must be used if you wish to use intermediary memory (can be done at any point in time). 
//Will return nil (no error) if memory is already initialized. 
func InitMemory() error {
	if Mem != nil {
		fmt.Println("InitMemory() -- already initialized, returning.")
		return nil
	}
	var err error
	Mem, err = sqlx.Connect("sqlite3", ":memory:")

	for _, source := range data_sources {

		db, err := sqlx.Connect("sqlite3", source.Path)
			if err != nil { return err }
			structure, err := structureFromDb(db)
			if err != nil { return err }
		db.Close()

		for tablename, table := range structure { //modified .tableCreateStatements()
			create_statement := "CREATE TABLE " + source.Alias + "_" + tablename 
			create_statement += "(" + strings.Join(table.Columns, ", ")
			if len(table.Primary_key) != 0 {
				create_statement += ", PRIMARY KEY("
				create_statement += strings.Join(table.Primary_key, ", ") + ")"
			}
			create_statement += ");"
	
			_, err := Mem.Exec(create_statement)
			if err != nil { return err }
		}
	}
	return err
}

//Saves all tables of memory into their respective databases
func SaveMemory() error {

	mem_structure, err := structureFromDb(Mem)
	if err != nil { return err }

	target_map := make(db_target_map)
	target_map[Mem] = make(map[*sqlx.DB]map[string]string)
	for mem_tablename := range mem_structure {
		alias_tablename := strings.SplitN(mem_tablename, "_", 2) //shouldn't be able to not make 2, mem_tablename is generated programatically, buuuut...
		if len(alias_tablename) != 2 {
			return ErrFatalSaveException
		}

		path, err := NameToPath(alias_tablename[0]) //shouldn't be able to error, buuut...
		if err != nil { return err }

		db, err := sqlx.Connect("sqlite3", path)
		if err != nil { return err }
		defer db.Close()

		if target_map[Mem][db] == nil { target_map[Mem][db] = make(map[string]string) }
		target_map[Mem][db][mem_tablename] = alias_tablename[1]
	}

	err = target_map.performMerge(false, "") 
	return err
}

//Deletes all of memory (WITHOUT SAVING IT!), setting it back to a nil pointer, and making all operations take place directly on files. 
//You may run InitMemory() to make a new Intermediary memory after this. 
//Will return nil (no error) if memory is already closed. 
func EndMemory() error {
	if Mem != nil {
		err := Mem.Close()
		if err != nil { return err }
		Mem = nil
	}
	return nil
}

//Checks if the schema of <name> is the same as that of <structure> . 
//strict = false -> Doesn't care about column/table names (only number, order & datatypes of columns matter). 
//strict = true -> Structures have to be equal in all aspects
func CheckSourceStructure(name string, structure Db_structure, strict bool) (bool, error) {
	path, err := NameToPath(name)
	if err != nil { return false, err }

	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return false, err }
	defer db.Close()

	if strict {
		db_structure, err := structureFromDb(db)
		if err != nil { return false, err }

		return reflect.DeepEqual(db_structure, structure), nil

	} else {
		_, compatible, err := dbToStructureCompat(db, structure, false) //maybe just reflect.DeepEqual() ?
		return compatible, err
	}
}


//Checks if the schema of <table> is the same as that of <db_table> . 
//Column names, datatypes, order, and primary key matter (everything). 
func CheckTableStructure(table string, db_table *Db_table) (bool, error) {
	name, tablename, err := FormatUInputTable(table)
	if err != nil { return false, err }

	path, err := NameToPath(name)
	if err != nil { return false, err }

	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return false, err } 
	defer db.Close()

	structure, err := structureFromDb(db) 
	if err != nil { return false, err }

	val, ok := structure[tablename]
	if !ok { return false, ErrUnknownTableName }

	if reflect.DeepEqual(val, db_table) {
		return  true, nil
	}

	return false, nil
}

//checks if the contents of <name> on disk are exactly the same as in memory. 
func CheckMemoryIsSaved(name string) (bool, error) {
	path, err := NameToPath(name)
	if err != nil { return false, err }

	alias, _ := NameToAlias(name) //don't care about err, since it would already be returned by NameToPath()

	db, err := sqlx.Connect("sqlite3", path)
	if err != nil { return false, err }
	defer db.Close()

	structure, err := structureFromDb(db)
	if err != nil { return false, err }

	joined_db, _, err := attachDatabases(Mem, db, "other") //don't care about swap since main is always in memory, and db has to be on disk
	if err != nil { return false, err }
	defer joined_db.Exec("DETACH DATABASE other;")

	for tablename := range structure {
		mem_tablename := alias + "_" + tablename

		var int_result int
		err = joined_db.Get(&int_result, fmt.Sprintf("SELECT NOT EXISTS (SELECT * FROM main.%[1]s EXCEPT SELECT * FROM other.%[2]s) AND NOT EXISTS (SELECT * FROM other.%[2]s EXCEPT SELECT * FROM main.%[1]s);", mem_tablename, tablename))
		if err != nil { return false, err }

		if int_result == 0 {
			return false, nil
		}
	}
	return true, nil
}

//returns the filepaths of all "outdated" (OutdateDataSource()) versions of <name> in <dir> sorted by age (newest first) (checks their structure to make sure their schemas are compatible)
func GetOutdated(name string, dir string) ([]string, error) {

	path, err := NameToPath(name)
	if err != nil { return []string{}, err }

	_, search_name := filepath.Split(path)
	search_name = search_name[: len(search_name) - 3]
	unf_re := fmt.Sprintf("^%s_[0-9]{20} ([0-9]{2}-[0-9]{2}-[0-9]{1,6}).db$", search_name) //will break once we get to year 1 000 000 (never happening ;) )
	re, err := regexp.Compile(unf_re)
	if err != nil { return []string{}, err }

	entries, err := os.ReadDir(dir)
	if err != nil { return []string{}, err }

	var sorted_paths []string
	for _, entry := range(entries){
		if re.MatchString(entry.Name()) && (filepath.Ext(entry.Name()) == ".db") {
			sorted_paths = append(sorted_paths, filepath.Join(dir, entry.Name())) //sorted by oldest first because os.ReadDir() returns files ordered by filename, and first variable of filename is time in millis
		}
	}
	slices.Reverse(sorted_paths) //Reverse so we get newest first

	return sorted_paths, nil
}



/*TODO:
- Add export to CSV
- Add export to checksum format or something (and integrate it)
- No real way to check if a database contains reserved tables/columns, add it
	-- maybe add to AddDataSource()
*/

/*quirks:
- No concurrency is present, and no checks for read/write locks exist, relying on sqlite3 for this
- Slow & Inefficient as frick (fixable)
*/