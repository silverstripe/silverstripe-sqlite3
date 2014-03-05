<?php

/**
 * SQLite connector class.
 * @package SQLite3
 */

class SQLitePDODatabase extends SQLite3Database {

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
		
		$dbName = !isset($this->database) ? $parameters['database'] : $dbName=$this->database;
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

		$this->dbConn = new PDO("sqlite:$file");

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

	public function query($sql, $errorLevel = E_USER_ERROR) {

		if(isset($_REQUEST['previewwrite']) && in_array(strtolower(substr($sql,0,strpos($sql,' '))), array('insert','update','delete','replace'))) {
			Debug::message("Will execute: $sql");
			return;
		}

		if(isset($_REQUEST['showqueries'])) { 
			$starttime = microtime(true);
		}

		// @todo This is a very ugly hack to rewrite the update statement of SiteTree::doPublish()
		// @see SiteTree::doPublish() There is a hack for MySQL already, maybe it's worth moving this to SiteTree or that other hack to Database...
		if(preg_replace('/[\W\d]*/i','',$sql) == 'UPDATESiteTree_LiveSETSortSiteTreeSortFROMSiteTreeWHERESiteTree_LiveIDSiteTreeIDANDSiteTree_LiveParentID') {
			preg_match('/\d+/i',$sql,$matches);
			$sql = 'UPDATE "SiteTree_Live"
				SET "Sort" = (SELECT "SiteTree"."Sort" FROM "SiteTree" WHERE "SiteTree_Live"."ID" = "SiteTree"."ID")
				WHERE "ParentID" = ' . $matches[0];
		}

		@$handle = $this->dbConn->query($sql);

		if(isset($_REQUEST['showqueries'])) {
			$endtime = round(microtime(true) - $starttime,4);
			Debug::message("\n$sql\n{$endtime}ms\n", false);
		}

		DB::$lastQuery=$handle;

		if(!$handle && $errorLevel) {
			$msg = $this->dbConn->errorInfo();
			$this->databaseError("Couldn't run query: $sql | " . $msg[2], $errorLevel);
		}

		return new SQLitePDOQuery($this, $handle);
	}

	public function getGeneratedID($table) {
		return $this->dbConn->lastInsertId();
	}

	/*
	 * This will return text which has been escaped in a database-friendly manner
	 */
	function addslashes($value){
		return str_replace("'", "''", $value);
	}
}

/**
 * A result-set from a SQLitePDO database.
 * @package SQLite3
 */
class SQLitePDOQuery extends SQLite3Query {

	/**
	 * Hook the result-set given into a Query class, suitable for use by framework.
	 * @param database The database object that created this query.
	 * @param handle the internal sqlitePDO handle that is points to the resultset.
	 */
	public function __construct(SQLitePDODatabase $database, PDOStatement $handle) {
		$this->database = $database;
		$this->handle = $handle;
	}

	public function __destruct() {
		if($this->handle) $this->handle->closeCursor();
	}

	public function __destroy() {
		$this->handle->closeCursor();
	}

	public function seek($row) {
		$this->handle->execute();
		$i=0;
		while($i < $row && $row = $this->handle->fetch()) $i++;
		return (bool) $row;
	}

	public function numRecords() {
		return $this->handle->rowCount();
	}

	public function nextRecord() {
		$this->handle->setFetchMode( PDO::FETCH_CLASS, 'ResultRow');
		if($data = $this->handle->fetch(PDO::FETCH_CLASS)) {
			foreach($data->get() as $columnName => $value) {
				if(preg_match('/^"([a-z0-9_]+)"\."([a-z0-9_]+)"$/i', $columnName, $matches)) $columnName = $matches[2];
				else if(preg_match('/^"([a-z0-9_]+)"$/i', $columnName, $matches)) $columnName = $matches[1];
				else $columnName = trim($columnName,"\"' \t");
				$output[$columnName] = is_null($value) ? null : (string)$value;
			}
			return $output;
		} else {
			return false;
		}
	}
}

/**
 * This is necessary for a case where we have ambigous fields in the result.
 * E.g. we have something like the following:
 * SELECT Child1.value, Child2.value FROM Parent LEFT JOIN Child1 LEFT JOIN Child2
 * We get value twice in the result set. We want the last not empty value.
 * The fetch assoc syntax does'nt work because it gives us the last value everytime, empty or not.
 * The fetch num does'nt work because there is no function to retrieve the field names to create the map.
 * In this approach we make use of PDO fetch class to pass the result values to an
 * object and let the __set() function do the magic decision to choose the right value.
 */
class ResultRow {
	private $_datamap=array();

	function __set($key,$val) {
		if($val || !isset($this->_datamap[$key])) $this->_datamap[$key] = $val;
	}

	function get() {
		return $this->_datamap;
	}
}
