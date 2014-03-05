<?php

/**
 * SQLite connector class.
 * @package SQLite3
 */

class SQLite3Database extends SS_Database {
	/**
	 * Connection to the DBMS.
	 * @var object
	 */
	protected $dbConn;

	/**
	 * True if we are connected to a database.
	 * @var boolean
	 */
	protected $active;

	/**
	 * The name of the database.
	 * @var string
	 */
	protected $database;

	/*
	 * This holds the name of the original database
	 * So if you switch to another for unit tests, you
	 * can then switch back in order to drop the temp database 
	 */
	protected $database_original;

	/*
	 * This holds the parameters that the original connection was created with,
	 * so we can switch back to it if necessary (used for unit tests)
	 */
	protected $parameters;

	public static $default_pragma = array();

	public static $vacuum = true;

	/**
	 * Connect to a SQLite3 database.
	 * @param array $parameters An map of parameters, which should include:
	 *  - database: The database to connect to
	 *  - path: the path to the SQLite3 database file
	 *  - key: the encryption key (needs testing)
	 *  - memory: use the faster In-Memory database for unit tests
	 */
	public function __construct($parameters) {
		//We will store these connection parameters for use elsewhere (ie, unit tests)
		$this->parameters=$parameters;
		$this->connectDatabase();

		$this->database_original=$this->database;
	}
	
	/*
	 * Uses whatever connection details are in the $parameters array to connect to a database of a given name
	 */
	function connectDatabase() {
		$this->enum_map = array();
		$parameters = $this->parameters;
		
		if(!empty($parameters['memory'])) {
			Deprecation::notice(
				'1.4.0',
				"\$databaseConfig['memory'] is deprecated. Use \$databaseConfig['path'] = ':memory:' instead.",
				Deprecation::SCOPE_GLOBAL
			);
			$parameters['path'] = ':memory:';
		}
		
		$dbName = !isset($this->database) ? $parameters['database'] : $this->database;
		$file = $parameters['path'];

		// assumes that the path to dbname will always be provided
		// this is only necessary if we're using a filesystem path, and not an in-memory database
		if($file != ':memory:') {
			$file = $parameters['path'] . '/' . $dbName;
			if(!file_exists($parameters['path'])) {
				SQLiteDatabaseConfigurationHelper::create_db_dir($parameters['path']);
				SQLiteDatabaseConfigurationHelper::secure_db_dir($parameters['path']);
			}
		}

		$this->dbConn = new SQLite3($file, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, $parameters['key']);
		if(method_exists('SQLite3', 'busyTimeout')) $this->dbConn->busyTimeout(60000);

		$this->active = true;
		$this->database = $dbName;

		if(!$this->dbConn) {
			$this->databaseError("Couldn't connect to SQLite3 database");
			return false;
		}
		
		foreach(self::$default_pragma as $pragma => $value) $this->pragma($pragma, $value);
		
		if(empty(self::$default_pragma['locking_mode'])) {
			self::$default_pragma['locking_mode'] = $this->pragma('locking_mode');
		}

		return true;
	}

	/**
	 * Not implemented, needed for PDO
	 */
	public function getConnect($parameters) {
		return null;
	}

	/**
	 * Returns true if this database supports collations
	 * TODO: get rid of this?
	 * @return boolean
	 */
	public function supportsCollations() {
		return true;
	}

	public function supportsTimezoneOverride() {
		return false;
	}

	/**
	 * Get the version of SQLite3.
	 * @return float
	 */
	public function getVersion() {
		return $this->query("SELECT sqlite_version()")->value();
	}

	/**
	 * Execute PRAGMA commands.
	 * works as getter and setter for connection params
	 * @param String pragma name
	 * @param String optional value to set
	 * @return String the pragma value
	 */
	protected function pragma($pragma, $value = null) {
		if(strlen($value)) {
			$this->query("PRAGMA $pragma = $value");
		} else {
			$value = $this->query("PRAGMA $pragma")->value();
		}

		return $value;
	}
	/**
	 * Get the database server, namely SQLite3.
	 * @return string
	 */
	public function getDatabaseServer() {
		return "SQLite3";
	}

	public function query($sql, $errorLevel = E_USER_ERROR) {

		if(isset($_REQUEST['previewwrite']) && in_array(strtolower(substr($sql,0,strpos($sql,' '))), array('insert','update','delete','replace'))) {
			Debug::message("Will execute: $sql");
			return;
		}

		if(isset($_REQUEST['showqueries'])) { 
			$starttime = microtime(true);
		}

		@$handle = $this->dbConn->query($sql);

		if(isset($_REQUEST['showqueries'])) {
			$endtime = round(microtime(true) - $starttime,4);
			Debug::message("\n$sql\n{$endtime}ms\n", false);
		}

		DB::$lastQuery=$handle;

		if(!$handle) {
			$this->databaseError("Couldn't run query: $sql | " . $this->dbConn->lastErrorMsg(), $errorLevel);
		}

		return new SQLite3Query($this, $handle);
	}

	public function getGeneratedID($table) {
		return $this->dbConn->lastInsertRowID();
	}

	/**
	 * OBSOLETE: Get the ID for the next new record for the table.
	 * 
	 * @var string $table The name od the table.
	 * @return int
	 */
	public function getNextID($table) {
		user_error('getNextID is OBSOLETE (and will no longer work properly)', E_USER_WARNING);
		$result = $this->query("SELECT MAX(ID)+1 FROM \"$table\"")->value();
		return $result ? $result : 1;
	}

	public function isActive() {
		return $this->active ? true : false;
	}

	/*
	 * This will create a database based on whatever is in the $this->database value
	 * So you need to have called $this->selectDatabase() first, or used the __construct method
	 */
	public function createDatabase() {

		$this->dbConn = null;
		$fullpath = $this->parameters['path'] . '/' . $this->database;
		if(is_writable($fullpath)) unlink($fullpath);

		$this->connectDatabase();

	}

	/**
	 * Drop the database that this object is currently connected to.
	 * Use with caution.
	 */
	public function dropDatabase() {
		//First, we need to switch back to the original database so we can drop the current one
		$this->dbConn = null;
		$db_to_drop=$this->database;
		$this->selectDatabase($this->database_original);
		$this->connectDatabase();

		$fullpath = $this->parameters['path'] . '/' . $db_to_drop;
		if(is_writable($fullpath)) unlink($fullpath);
	}

	/**
	 * Returns the name of the currently selected database
	 */
	public function currentDatabase() {
		return $this->database;
	}

	/**
	 * Switches to the given database.
	 * If the database doesn't exist, you should call createDatabase() after calling selectDatabase()
	 */
	public function selectDatabase($dbname) {
		$this->database=$dbname;

		$this->tableList = $this->fieldList = $this->indexList = null;

		return true;
	}


	/**
	 * Returns true if the named database exists.
	 */
	public function databaseExists($name) {
		$SQL_name=Convert::raw2sql($name);
		$result=$this->query("PRAGMA database_list");
		foreach($result as $db) if($db['name'] == 'main' && preg_match('/\/' . $name . '/', $db['file'])) return true;
		if(file_exists($this->parameters['path'] . '/' . $name)) return true;
		return false;
	}

	function beginSchemaUpdate() {
		$this->pragma('locking_mode', 'EXCLUSIVE');
		$this->checkAndRepairTable();
		// if($this->TableExists('SQLiteEnums')) $this->query("DELETE FROM SQLiteEnums");
		$this->checkAndRepairTable();
		parent::beginSchemaUpdate();
	}

	function endSchemaUpdate() {
		parent::endSchemaUpdate();
		$this->pragma('locking_mode', self::$default_pragma['locking_mode']);
	}

	public function clearTable($table) {
		if($table != 'SQLiteEnums') $this->dbConn->query("DELETE FROM \"$table\"");
	}

	public function createTable($table, $fields = null, $indexes = null, $options = null, $advancedOptions = null) {

		if(!isset($fields['ID'])) $fields['ID'] = $this->IdColumn();

		$fieldSchemata = array();
		if($fields) foreach($fields as $k => $v) {
			$fieldSchemata[] = "\"$k\" $v";
		}
		$fieldSchemas = implode(",\n",$fieldSchemata);

		// Switch to "CREATE TEMPORARY TABLE" for temporary tables
		$temporary = empty($options['temporary']) ? "" : "TEMPORARY";
		$this->query("CREATE $temporary TABLE \"$table\" (
			$fieldSchemas
		)");

		if($indexes) {
			foreach($indexes as $indexName => $indexDetails) {
				$this->createIndex($table, $indexName, $indexDetails);
			}
		}

		return $table;
	}

	/**
	 * Alter a table's schema.
	 * @param $table The name of the table to alter
	 * @param $newFields New fields, a map of field name => field schema
	 * @param $newIndexes New indexes, a map of index name => index type
	 * @param $alteredFields Updated fields, a map of field name => field schema
	 * @param $alteredIndexes Updated indexes, a map of index name => index type
	 */
	public function alterTable($tableName, $newFields = null, $newIndexes = null, $alteredFields = null, $alteredIndexes = null, $alteredOptions = null, $advancedOptions = null) {

		if($newFields) foreach($newFields as $fieldName => $fieldSpec) $this->createField($tableName, $fieldName, $fieldSpec);

		if($alteredFields) foreach($alteredFields as $fieldName => $fieldSpec) $this->alterField($tableName, $fieldName, $fieldSpec);

		if($newIndexes) foreach($newIndexes as $indexName => $indexSpec) $this->createIndex($tableName, $indexName, $indexSpec);

		if($alteredIndexes) foreach($alteredIndexes as $indexName => $indexSpec) $this->alterIndex($tableName, $indexName, $indexSpec);

	}
    
	public function renameTable($oldTableName, $newTableName) {

		$this->query("ALTER TABLE \"$oldTableName\" RENAME TO \"$newTableName\"");

	}

	protected static $checked_and_repaired = false;

	/**
	 * Repairs and reindexes the table.  This might take a long time on a very large table.
	 * @var string $tableName The name of the table.
	 * @return boolean Return true if the table has integrity after the method is complete.
	 */
	public function checkAndRepairTable($tableName = null) {
		$ok = true;

		if(!SapphireTest::using_temp_db() && !self::$checked_and_repaired) {
			$this->alterationMessage("Checking database integrity","repaired");
			if($msgs = $this->query('PRAGMA integrity_check')) foreach($msgs as $msg) if($msg['integrity_check'] != 'ok') { Debug::show($msg['integrity_check']); $ok = false; }
			if(self::$vacuum) {
				$this->query('VACUUM', E_USER_NOTICE);
				if($this instanceof SQLitePDODatabase) {
					$msg = $this->dbConn->errorInfo();
					$msg = isset($msg[2]) ? $msg[2] : 'no errors';
				} else {
					$msg = $this->dbConn->lastErrorMsg();
				}
				if(preg_match('/authoriz/', $msg)) {
					$this->alterationMessage('VACUUM | ' . $msg, "error");
				} else {
					$this->alterationMessage("VACUUMing", "repaired");
				}
			}
			self::$checked_and_repaired = true;
		}
		
		return $ok;
	}

	public function createField($tableName, $fieldName, $fieldSpec) {
		$this->query("ALTER TABLE \"$tableName\" ADD \"$fieldName\" $fieldSpec");
	}

	/**
	 * Change the database type of the given field.
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $fieldName The name of the field to change.
	 * @param string $fieldSpec The new field specification
	 */
	public function alterField($tableName, $fieldName, $fieldSpec) {

		$oldFieldList = $this->fieldList($tableName);
		$fieldNameList = '"' . implode('","', array_keys($oldFieldList)) . '"';

		if(!empty($_REQUEST['avoidConflict']) && Director::isDev()) $fieldSpec = preg_replace('/\snot null\s/i', ' NOT NULL ON CONFLICT REPLACE ', $fieldSpec);

		if(array_key_exists($fieldName, $oldFieldList)) {

			$oldCols = array();

			foreach($oldFieldList as $name => $spec) {
				$newColsSpec[] = "\"$name\" " . ($name == $fieldName ? $fieldSpec : $spec);
			}

			$queries = array(
				"BEGIN TRANSACTION",
				"CREATE TABLE \"{$tableName}_alterfield_{$fieldName}\"(" . implode(',', $newColsSpec) . ")",
				"INSERT INTO \"{$tableName}_alterfield_{$fieldName}\" SELECT {$fieldNameList} FROM \"$tableName\"",
				"DROP TABLE \"$tableName\"",
				"ALTER TABLE \"{$tableName}_alterfield_{$fieldName}\" RENAME TO \"$tableName\"",
				"COMMIT"
			);

			$indexList = $this->indexList($tableName);
			foreach($queries as $query) $this->query($query.';');

			foreach($indexList as $indexName => $indexSpec) $this->createIndex($tableName, $indexName, $indexSpec);

		}

	}

	/**
	 * Change the database column name of the given field.
	 * 
	 * @param string $tableName The name of the tbale the field is in.
	 * @param string $oldName The name of the field to change.
	 * @param string $newName The new name of the field
	 */
	public function renameField($tableName, $oldName, $newName) {
		$oldFieldList = $this->fieldList($tableName);
		$oldCols = array();

		if(array_key_exists($oldName, $oldFieldList)) {
			foreach($oldFieldList as $name => $spec) {
				$oldCols[] = "\"$name\"" . (($name == $oldName) ? " AS $newName" : '');
				$newCols[] = "\"". (($name == $oldName) ? $newName : $name). "\"";
				$newColsSpec[] = "\"" . (($name == $oldName) ? $newName : $name) . "\" $spec";
			}

			// SQLite doesn't support direct renames through ALTER TABLE
			$queries = array(
				"BEGIN TRANSACTION",
				"CREATE TABLE \"{$tableName}_renamefield_{$oldName}\" (" . implode(',', $newColsSpec) . ")",
				"INSERT INTO \"{$tableName}_renamefield_{$oldName}\" SELECT " . implode(',', $oldCols) . " FROM \"$tableName\"",
				"DROP TABLE \"$tableName\"",
				"ALTER TABLE \"{$tableName}_renamefield_{$oldName}\" RENAME TO \"$tableName\"",
				"COMMIT"
			);

			// Remember original indexes
			$oldIndexList = $this->indexList($tableName);

			// Then alter the table column
			foreach($queries as $query) $this->query($query.';');

			// Recreate the indexes
			foreach($oldIndexList as $indexName => $indexSpec) {
				$renamedIndexSpec = array();
				foreach(explode(',', $indexSpec) as $col) {
					$col = trim($col, '"'); // remove quotes
					$renamedIndexSpec[] = ($col == $oldName) ? $newName : $col;
				}
				$this->createIndex($tableName, $indexName, implode(',', $renamedIndexSpec));
			}
		}
	}

	public function fieldList($table) {
		$sqlCreate = DB::query('SELECT sql FROM sqlite_master WHERE type = "table" AND name = "' . $table . '"')->record();
		$fieldList = array();

		if($sqlCreate && $sqlCreate['sql']) {
			preg_match('/^[\s]*CREATE[\s]+TABLE[\s]+[\'"]?[a-zA-Z0-9_\\\]+[\'"]?[\s]*\((.+)\)[\s]*$/ims', $sqlCreate['sql'], $matches);
			$fields = isset($matches[1]) ? preg_split('/,(?=(?:[^\'"]*$)|(?:[^\'"]*[\'"][^\'"]*[\'"][^\'"]*)*$)/x', $matches[1]) : array();
			foreach($fields as $field) {
				$details = preg_split('/\s/', trim($field));
				$name = array_shift($details);
				$name = str_replace('"', '', trim($name));
				$fieldList[$name] = implode(' ', $details);
			}
		}

		return $fieldList;
	}

	/**
	 * Create an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see Database::requireIndex() for more details.
	 */
	public function createIndex($tableName, $indexName, $indexSpec) {
		$spec = $this->convertIndexSpec($indexSpec, $indexName);
		if(!preg_match('/".+"/', $indexName)) $indexName = "\"$indexName\"";
		
		$this->query("CREATE INDEX IF NOT EXISTS $indexName ON \"$tableName\" ($spec)");

	}

	/*
	 * This takes the index spec which has been provided by a class (ie static $indexes = blah blah)
	 * and turns it into a proper string.
	 * Some indexes may be arrays, such as fulltext and unique indexes, and this allows database-specific
	 * arrays to be created.
	 */
	public function convertIndexSpec($indexSpec, $indexName = null) {
		if(is_array($indexSpec)) {
			$indexSpec = $indexSpec['value'];
		} else if(is_numeric($indexSpec)) {
			$indexSpec = $indexName;
		}
		
		if(preg_match('/\((.+)\)/', $indexSpec, $matches)) {
			$indexSpec = $matches[1];
		}

		return preg_replace('/\s/', '', $indexSpec);
	}

	/**
	 * prefix indexname with uppercase tablename if not yet done, in order to avoid ambiguity
	 */
	function getDbSqlDefinition($tableName, $indexName, $indexSpec) {
		return "\"$tableName.$indexName\"";
	}

	/**
	 * Alter an index on a table.
	 * @param string $tableName The name of the table.
	 * @param string $indexName The name of the index.
	 * @param string $indexSpec The specification of the index, see Database::requireIndex() for more details.
	 */
	public function alterIndex($tableName, $indexName, $indexSpec) {
		$this->createIndex($tableName, $indexName, $indexSpec);
	}

	/**
	 * Return the list of indexes in a table.
	 * @param string $table The table name.
	 * @return array
	 */
	public function indexList($table) {
		$indexList = array();
		foreach(DB::query('PRAGMA index_list("' . $table . '")') as $index) {
			$list = array();
			foreach(DB::query('PRAGMA index_info("' . $index["name"] . '")') as $details) $list[] = $details['name'];
			$indexList[$index["name"]] = implode(',', $list);
		}
		foreach($indexList as $name => $val) {
			// Normalize quoting to avoid false positives when checking for index changes
			// during schema generation
			$valParts = preg_split('/\s*,\s*/', $val);
			foreach($valParts as $i => $valPart) {
				$valParts[$i] = preg_replace('/^"?(.*)"?$/', '$1', $valPart);
			}
				
			$indexList[$name] = '"' . implode('","', $valParts) . '"';
		}

		return $indexList;
	}

	/**
	 * Returns a list of all the tables in the database.
	 * Table names will all be in lowercase.
	 * @return array
	 */
	public function tableList() {
		$tables = array();
		foreach($this->query('SELECT name FROM sqlite_master WHERE type = "table"') as $record) {
			//$table = strtolower(reset($record));
			$table = reset($record);
			$tables[strtolower($table)] = $table;
		}

		//Return an empty array if there's nothing in this database
		return isset($tables) ? $tables : Array();
	}

	function TableExists($tableName){

		$result=$this->query('SELECT name FROM sqlite_master WHERE type = "table" AND name="' . $tableName . '"')->first();

		if($result)
			return true;
		else
			return false;

	}

	/**
	 * Return the number of rows affected by the previous operation.
	 * @return int
	 */
	public function affectedRows() {
		return $this->dbConn->changes();
	}

	/**
	 * Return a boolean type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function boolean($values){

		return 'BOOL NOT NULL DEFAULT ' . (isset($values['default']) ? (int)$values['default'] : 0);

	}

	/**
	 * Return a date type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function date($values){

		return "TEXT";

	}

	/**
	 * Return a decimal type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function decimal($values, $asDbValue=false){

		$default = isset($values['default']) && is_numeric($values['default']) ? $values['default'] : 0;
		return "NUMERIC NOT NULL DEFAULT " . $default;

	}

	/**
	 * Return a enum type-formatted string
	 *
 	 * enumus are not supported. as a workaround to store allowed values we creates an additional table
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	protected $enum_map = array();
	
	public function enum($values){
		$tablefield = $values['table'] . '.' . $values['name'];
		if(empty($this->enum_map)) $this->query("CREATE TABLE IF NOT EXISTS \"SQLiteEnums\" (\"TableColumn\" TEXT PRIMARY KEY, \"EnumList\" TEXT)");
		if(empty($this->enum_map[$tablefield]) || $this->enum_map[$tablefield] != implode(',', $values['enums'])) {
			$this->query("REPLACE INTO SQLiteEnums (TableColumn, EnumList) VALUES (\"{$tablefield}\", \"" . implode(',', $values['enums']) . "\")");
			$this->enum_map[$tablefield] = implode(',', $values['enums']);
		}
		return "TEXT DEFAULT '{$values['default']}'";
	}
	
	/**
	 * Return a set type-formatted string
	 * This type doesn't exist in SQLite as well
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function set($values) {
		$tablefield = $values['table'] . '.' . $values['name'];
		if(empty($this->enum_map)) $this->query("CREATE TABLE IF NOT EXISTS SQLiteEnums (TableColumn TEXT PRIMARY KEY, EnumList TEXT)");
		if(empty($this->enum_map[$tablefield]) || $this->enum_map[$tablefield] != implode(',', $values['enums'])) {
			$this->query("REPLACE INTO SQLiteEnums (TableColumn, EnumList) VALUES (\"{$tablefield}\", \"" . implode(',', $values['enums']) . "\")");
			$this->enum_map[$tablefield] = implode(',', $values['enums']);
		}
		$default = '';
		if(!empty($values['default'])) {
			$default = str_replace(array('"',"'","\\","\0"), "", $values['default']);
			$default = " DEFAULT '$default'";
		}
		return 'TEXT' . $default;
	}

	/**
	 * Return a float type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function float($values, $asDbValue=false){

		return "REAL";

	}

	/**
	 * Return a Double type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function Double($values, $asDbValue=false){

		return "REAL";

	}

	/**
	 * Return a int type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function int($values, $asDbValue=false){

		return "INTEGER({$values['precision']}) " . strtoupper($values['null']) . " DEFAULT " . (int)$values['default'];

	}

	/**
	 * Return a datetime type-formatted string
	 * For SQLite3, we simply return the word 'TEXT', no other parameters are necessary
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function ss_datetime($values, $asDbValue=false){

		return "DATETIME";

	}

	/**
	 * Return a text type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function text($values, $asDbValue=false){

		return 'TEXT';

	}

	/**
	 * Return a time type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function time($values){

		return "TEXT";

	}

	/**
	 * Return a varchar type-formatted string
	 * 
	 * @params array $values Contains a tokenised list of info about this data type
	 * @return string
	 */
	public function varchar($values, $asDbValue=false){
		return "VARCHAR({$values['precision']}) COLLATE NOCASE";
	}

	/*
	 * Return a 4 digit numeric type.  MySQL has a proprietary 'Year' type.
	 * For SQLite3 we use TEXT
	 */
	public function year($values, $asDbValue=false){

		return "TEXT";

	}

	function escape_character($escape=false){

		if($escape) return "\\\""; else return "\"";

	}

	/**
	 * This returns the column which is the primary key for each table
	 * In SQLite3 it is INTEGER PRIMARY KEY AUTOINCREMENT
	 * SQLite3 does autoincrement ids even without the AUTOINCREMENT keyword, but the behaviour is signifficantly different
	 *
	 * @return string
	 */
	function IdColumn($asDbValue=false){
		return 'INTEGER PRIMARY KEY AUTOINCREMENT';
	}

	/**
	 * Returns true if this table exists
	 */
	function hasTable($tableName) {
		$SQL_table = Convert::raw2sql($tableName);
		return (bool)($this->query("SELECT name FROM sqlite_master WHERE type = \"table\" AND name = \"$tableName\"")->value());
	}

	/**
	 * Returns the SQL command to get all the tables in this database
	 */
	function allTablesSQL(){
		return 'SELECT name FROM sqlite_master WHERE type = "table"';
	}

	/**
	 * Return enum values for the given field
	 * @return array
	 */
	public function enumValuesForField($tableName, $fieldName) {
		$classnameinfo = DB::query("SELECT EnumList FROM SQLiteEnums WHERE TableColumn = \"{$tableName}.{$fieldName}\"")->first();
		$output = array();
		if($classnameinfo) {
			$output = explode(',', $classnameinfo['EnumList']);
		}
		return $output;
	}

	/**
	 * Get the actual enum fields from the constraint value:
	 */
	protected function EnumValuesFromConstraint($constraint){
		$constraint=substr($constraint, strpos($constraint, 'ANY (ARRAY[')+11);
		$constraint=substr($constraint, 0, -11);
		$constraints=Array();
		$segments=explode(',', $constraint);
		foreach($segments as $this_segment){
			$bits=preg_split('/ *:: */', $this_segment);
			array_unshift($constraints, trim($bits[0], " '"));
		}

		return $constraints;
	}

	/*
	 * Returns the database-specific version of the now() function
	 */
	function now(){
		return "datetime('now', 'localtime')";
	}

	/*
	 * Returns the database-specific version of the random() function
	 */
	function random(){
		return 'random()';
	}

	/*
	 * This is a lookup table for data types.
	 * For instance, Postgres uses 'INT', while MySQL uses 'UNSIGNED'
	 * So this is a DB-specific list of equivalents.
	 */
	function dbDataType($type){
		$values=Array(
			'unsigned integer'=>'INT'
		);
		
		if(isset($values[$type]))
			return $values[$type];
		else return '';
	}
	
	/*
	 * This will return text which has been escaped in a database-friendly manner
	 */
	function addslashes($value){
		return $this->dbConn->escapeString($value);
	}

	/*
	 * This changes the index name depending on database requirements.
	 */
	function modifyIndex($index, $spec){
		return str_replace('"', '', $index);
	}

	/**
	 * The core search engine configuration.
	 * @todo There is a fulltext search for SQLite making use of virtual tables, the fts3 extension and the MATCH operator
	 * there are a few issues with fts:
	 * - shared cached lock doesn't allow to create virtual tables on versions prior to 3.6.17
	 * - there must not be more than one MATCH operator per statement
	 * - the fts3 extension needs to be available
	 * for now we use the MySQL implementation with the MATCH()AGAINST() uglily replaced with LIKE
	 * 
	 * @param string $keywords Keywords as a space separated string
	 * @return object DataObjectSet of result pages
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "Relevance DESC", $extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false) {
		$fileFilter = '';
		$keywords = Convert::raw2sql(str_replace(array('*','+','-','"','\''),'',$keywords));
		$htmlEntityKeywords = htmlentities(utf8_decode($keywords));

		$extraFilters = array('SiteTree' => '', 'File' => '');

		if($extraFilter) {
			$extraFilters['SiteTree'] = " AND $extraFilter";

			if($alternativeFileFilter) $extraFilters['File'] = " AND $alternativeFileFilter";
			else $extraFilters['File'] = $extraFilters['SiteTree'];
		}

		// Always ensure that only pages with ShowInSearch = 1 can be searched
		$extraFilters['SiteTree'] .= ' AND ShowInSearch <> 0';
		// File.ShowInSearch was added later, keep the database driver backwards compatible 
		// by checking for its existence first
		$fields = $this->fieldList('File');
		if(array_key_exists('ShowInSearch', $fields)) {
			$extraFilters['File'] .= " AND ShowInSearch <> 0";
		}

		$limit = $start . ", " . (int) $pageLength;

		$notMatch = $invertedMatch ? "NOT " : "";
		if($keywords) {
			$match['SiteTree'] = "
				(Title LIKE '%$keywords%' OR MenuTitle LIKE '%$keywords%' OR Content LIKE '%$keywords%' OR MetaDescription LIKE '%$keywords%' OR
				Title LIKE '%$htmlEntityKeywords%' OR MenuTitle LIKE '%$htmlEntityKeywords%' OR Content LIKE '%$htmlEntityKeywords%' OR MetaDescription LIKE '%$htmlEntityKeywords%')
			";
			$match['File'] = "(Filename LIKE '%$keywords%' OR Title LIKE '%$keywords%' OR Content LIKE '%$keywords%') AND ClassName = 'File'";

			// We make the relevance search by converting a boolean mode search into a normal one
			$relevanceKeywords = $keywords;
			$htmlEntityRelevanceKeywords = $htmlEntityKeywords;
			$relevance['SiteTree'] = "(Title LIKE '%$relevanceKeywords%' OR MenuTitle LIKE '%$relevanceKeywords%' OR Content LIKE '%$relevanceKeywords%' OR MetaDescription LIKE '%$relevanceKeywords%') + (Title LIKE '%$htmlEntityRelevanceKeywords%' OR MenuTitle LIKE '%$htmlEntityRelevanceKeywords%' OR Content LIKE '%$htmlEntityRelevanceKeywords%' OR MetaDescription LIKE '%$htmlEntityRelevanceKeywords%')";
			$relevance['File'] = "(Filename LIKE '%$relevanceKeywords%' OR Title LIKE '%$relevanceKeywords%' OR Content LIKE '%$relevanceKeywords%')";
		} else {
			$relevance['SiteTree'] = $relevance['File'] = 1;
			$match['SiteTree'] = $match['File'] = "1 = 1";
		}

		// Generate initial queries and base table names
		$baseClasses = array('SiteTree' => '', 'File' => '');
		$queries = array();
		foreach($classesToSearch as $class) {
			$queries[$class] = DataList::create($class)->where($notMatch . $match[$class] . $extraFilters[$class], "")->dataQuery()->query();
			$fromArr = $queries[$class]->getFrom();
			$baseClasses[$class] = reset($fromArr);
		}

		// Make column selection lists
		$select = array(
			'SiteTree' => array(
				"\"ClassName\"",
				"\"ID\"",
				"\"ParentID\"",
				"\"Title\"",
				"\"URLSegment\"",
				"\"Content\"",
				"\"LastEdited\"",
				"\"Created\"",
				"NULL AS \"Filename\"",
				"NULL AS \"Name\"",
				"\"CanViewType\"",
				"$relevance[SiteTree] AS Relevance"
			),
			'File' => array(
				"\"ClassName\"",
				"\"ID\"",
				"NULL AS \"ParentID\"",
				"\"Title\"",
				"NULL AS \"URLSegment\"",
				"\"Content\"",
				"\"LastEdited\"",
				"\"Created\"",
				"\"Filename\"",
				"\"Name\"",
				"NULL AS \"CanViewType\"",
				"$relevance[File] AS Relevance"
			)
		);

		// Process queries
		foreach($classesToSearch as $class) {
			// There's no need to do all that joining
			$queries[$class]->setFrom($baseClasses[$class]);

			$queries[$class]->setSelect(array());
			foreach($select[$class] as $clause) {
				if(preg_match('/^(.*) +AS +"?([^"]*)"?/i', $clause, $matches)) {
					$queries[$class]->selectField($matches[1], $matches[2]);
				} else {
					$queries[$class]->selectField(str_replace('"', '', $clause));
				}
			}

			$queries[$class]->setOrderBy(array());
		}

		// Combine queries
		$querySQLs = array();
		$totalCount = 0;
		foreach($queries as $query) {
			$querySQLs[] = $query->sql();
			$totalCount += $query->unlimitedRowCount();
		}

		$fullQuery = implode(" UNION ", $querySQLs) . " ORDER BY $sortBy LIMIT $limit";
		// Get records
		$records = DB::query($fullQuery);

		foreach($records as $record) {
			$objects[] = new $record['ClassName']($record);
		}

		if(isset($objects)) $doSet = new ArrayList($objects);
		else $doSet = new ArrayList();
		$list = new PaginatedList($doSet);
		$list->setPageStart($start);
		$list->setPageLEngth($pageLength);
		$list->setTotalItems($totalCount);
		return $list;
	}

	/*
	 * Does this database support transactions?
	 */
	public function supportsTransactions(){
		return version_compare($this->getVersion(), '3.6', '>=');
	}

	/*
	 * This is a quick lookup to discover if the database supports particular extensions
	 */
	public function supportsExtensions($extensions=Array('partitions', 'tablespaces', 'clustering')){

		if(isset($extensions['partitions']))
			return true;
		elseif(isset($extensions['tablespaces']))
			return true;
		elseif(isset($extensions['clustering']))
			return true;
		else
			return false;
	}

	/**
	 * @deprecated 1.2 use transactionStart() (method required for 2.4.x)
	 */
	public function startTransaction($transaction_mode=false, $session_characteristics=false){
		$this->transactionStart($transaction_mode, $session_characteristics);
	}

	/*
	 * Start a prepared transaction
	 */
	public function transactionStart($transaction_mode=false, $session_characteristics=false){
		DB::query('BEGIN');
	}

	/*
	 * Create a savepoint that you can jump back to if you encounter problems
	 */
	public function transactionSavepoint($savepoint){
		DB::query("SAVEPOINT \"$savepoint\"");
	}

	/*
	 * Rollback or revert to a savepoint if your queries encounter problems
	 * If you encounter a problem at any point during a transaction, you may
	 * need to rollback that particular query, or return to a savepoint
	 */
	public function transactionRollback($savepoint=false){

		if($savepoint) {
			DB::query("ROLLBACK TO $savepoint;");
		} else {
			DB::query('ROLLBACK;');
		}
	}

	/**
	 * @deprecated 1.2 use transactionEnd() (method required for 2.4.x)
	 */
	public function endTransaction(){
		$this->transactionEnd();
	}

	/*
	 * Commit everything inside this transaction so far
	 */
	public function transactionEnd(){
		DB::query('COMMIT;');
	}

	/**
	 *
	 * This is a stub function.  Postgres caches the fieldlist results.
	 *
	 * @param string $tableName
	 *
	 * @return boolean
	 */
	function clear_cached_fieldlist($tableName=false){
		return true;
	}

	/**
	 * Generate a WHERE clause for text matching.
	 * 
	 * @param String $field Quoted field name
	 * @param String $value Escaped search. Can include percentage wildcards.
	 * @param boolean $exact Exact matches or wildcard support.
	 * @param boolean $negate Negate the clause.
	 * @param boolean $caseSensitive Enforce case sensitivity if TRUE or FALSE.
	 *                               Stick with default collation if set to NULL.
	 * @return String SQL
	 */
	public function comparisonClause($field, $value, $exact = false, $negate = false, $caseSensitive = null) {
		if($exact && !$caseSensitive) {
			$comp = ($negate) ? '!=' : '=';
		} else {
			if($caseSensitive) {
				// GLOB uses asterisks as wildcards.
				// Replace them in search string, without replacing escaped percetage signs.
				$comp = 'GLOB';
				$value = preg_replace('/^%([^\\\\])/', '*$1', $value);
				$value = preg_replace('/([^\\\\])%$/', '$1*', $value);
				$value = preg_replace('/([^\\\\])%/', '$1*', $value);
			} else {
				$comp = 'LIKE';
			}
			if($negate) $comp = 'NOT ' . $comp;
		}
		
		return sprintf("%s %s '%s'", $field, $comp, $value);
	}

	/**
	 * Function to return an SQL datetime expression that can be used with SQLite3
	 * used for querying a datetime in a certain format
	 * @param string $date to be formated, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $format to be used, supported specifiers:
	 * %Y = Year (four digits)
	 * %m = Month (01..12)
	 * %d = Day (01..31)
	 * %H = Hour (00..23)
	 * %i = Minutes (00..59)
	 * %s = Seconds (00..59)
	 * %U = unix timestamp, can only be used on it's own
	 * @return string SQL datetime expression to query for a formatted datetime
	 */
	function formattedDatetimeClause($date, $format) {

		preg_match_all('/%(.)/', $format, $matches);
		foreach($matches[1] as $match) if(array_search($match, array('Y','m','d','H','i','s','U')) === false) user_error('formattedDatetimeClause(): unsupported format character %' . $match, E_USER_WARNING);
		
		$translate = array(
			'/%i/' => '%M',
			'/%s/' => '%S',
			'/%U/' => '%s',
		);
		$format = preg_replace(array_keys($translate), array_values($translate), $format);

		$modifiers = array();
		if($format == '%s' && $date != 'now') $modifiers[] = 'utc';
		if($format != '%s' && $date == 'now') $modifiers[] = 'localtime';

		if(preg_match('/^now$/i', $date)) {
			$date = "'now'";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "'$date'";
		}

		$modifier = empty($modifiers) ? '' : ", '" . implode("', '", $modifiers) . "'";
		return "strftime('$format', $date$modifier)";
	}
	
	/**
	 * Function to return an SQL datetime expression that can be used with SQLite3
	 * used for querying a datetime addition
	 * @param string $date, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $interval to be added, use the format [sign][integer] [qualifier], e.g. -1 Day, +15 minutes, +1 YEAR
	 * supported qualifiers:
	 * - years
	 * - months
	 * - days
	 * - hours
	 * - minutes
	 * - seconds
	 * This includes the singular forms as well
	 * @return string SQL datetime expression to query for a datetime (YYYY-MM-DD hh:mm:ss) which is the result of the addition
	 */
	function datetimeIntervalClause($date, $interval) {
		$modifiers = array();
		if($date == 'now') $modifiers[] = 'localtime';

		if(preg_match('/^now$/i', $date)) {
			$date = "'now'";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date)) {
			$date = "'$date'";
		}

		$modifier = empty($modifiers) ? '' : ", '" . implode("', '", $modifiers) . "'";
		return "datetime($date$modifier, '$interval')";
	}

	/**
	 * Function to return an SQL datetime expression that can be used with SQLite3
	 * used for querying a datetime substraction
	 * @param string $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @param string $date2 to be substracted of $date1, can be either 'now', literal datetime like '1973-10-14 10:30:00' or field name, e.g. '"SiteTree"."Created"'
	 * @return string SQL datetime expression to query for the interval between $date1 and $date2 in seconds which is the result of the substraction
	 */
	function datetimeDifferenceClause($date1, $date2) {

		$modifiers1 = array();
		$modifiers2 = array();

		if($date1 == 'now') $modifiers1[] = 'localtime';
		if($date2 == 'now') $modifiers2[] = 'localtime';

		if(preg_match('/^now$/i', $date1)) {
			$date1 = "'now'";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date1)) {
			$date1 = "'$date1'";
		}

		if(preg_match('/^now$/i', $date2)) {
			$date2 = "'now'";
		} else if(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/i', $date2)) {
			$date2 = "'$date2'";
		}

		$modifier1 = empty($modifiers1) ? '' : ", '" . implode("', '", $modifiers1) . "'";
		$modifier2 = empty($modifiers2) ? '' : ", '" . implode("', '", $modifiers2) . "'";

		return "strftime('%s', $date1$modifier1) - strftime('%s', $date2$modifier2)";
	}
}

/**
 * A result-set from a SQLite3 database.
 * @package SQLite3Database
 */
class SQLite3Query extends SS_Query {

	/**
	 * The SQLite3Database object that created this result set.
	 * @var SQLite3Database
	 */
	protected $database;

	/**
	 * The internal sqlite3 handle that points to the result set.
	 * @var resource
	 */
	protected $handle;

	/**
	 * Hook the result-set given into a Query class, suitable for use by framework.
	 * @param database The database object that created this query.
	 * @param handle the internal sqlite3 handle that is points to the resultset.
	 */
	public function __construct(SQLite3Database $database, $handle) {
		$this->database = $database;
		$this->handle = $handle;
	}

	public function __destruct() {
		if($this->handle) $this->handle->finalize();
	}

	public function seek($row) {
		$this->handle->reset();
		$i=0;
		while($i < $row && $row = @$this->handle->fetchArray()) $i++;
		return true;
	}

	/**
	 * @todo This looks terrible but there is no SQLite3::get_num_rows() implementation
	 */
	public function numRecords() {
		$c=0;
		while($this->handle->fetchArray()) $c++;
		$this->handle->reset();
		return $c;
	}

	public function nextRecord() {
		if($data = $this->handle->fetchArray(SQLITE3_ASSOC)) {
			return $data;
		} else {
			return false;
		}
	}
}
