<?php

/**
 * SQLite database controller class
 * 
 * @package SQLite3
 */
class SQLite3Database extends SS_Database {

	/**
	 * Database schema manager object
	 * 
	 * @var SQLite3SchemaManager
	 */
	protected $schemaManager = null;

	/*
	 * This holds the parameters that the original connection was created with,
	 * so we can switch back to it if necessary (used for unit tests)
	 * 
	 * @var array
	 */
	protected $parameters;

	/*
	 * if we're on a In-Memory db
	 * 
	 * @var boolean
	 */
	protected $livesInMemory = false;

	/**
	 * List of default pragma values
	 * 
	 * @todo Migrate to SS config
	 *
	 * @var array
	 */
	public static $default_pragma = array(
		'encoding' => '"UTF-8"',
		'locking_mode' => 'NORMAL'
	);


	/**
	 * Extension used to distinguish between sqllite database files and other files.
	 * Required to handle multiple databases.
	 * 
	 * @return string
	 */
	public static function database_extension() {
		return Config::inst()->get('SQLite3Database', 'database_extension');
	}

	/**
	 * Check if a database name has a valid extension
	 * 
	 * @param string $name
	 * @return boolean
	 */
	public static function is_valid_database_name($name) {
		$extension = self::database_extension();
		if(empty($extension)) return true;

		return substr_compare($name, $extension, -strlen($extension), strlen($extension)) === 0;
	}

	/**
	 * Connect to a SQLite3 database.
	 * @param array $parameters An map of parameters, which should include:
	 *  - database: The database to connect to, with the correct file extension (.sqlite)
	 *  - path: the path to the SQLite3 database file
	 *  - key: the encryption key (needs testing)
	 *  - memory: use the faster In-Memory database for unit tests
	 */
	public function connect($parameters) {
		
		if(!empty($parameters['memory'])) {
			Deprecation::notice(
				'1.4.0',
				"\$databaseConfig['memory'] is deprecated. Use \$databaseConfig['path'] = ':memory:' instead.",
				Deprecation::SCOPE_GLOBAL
			);
			unset($parameters['memory']);
			$parameters['path'] = ':memory:';
		}
		
		//We will store these connection parameters for use elsewhere (ie, unit tests)
		$this->parameters = $parameters;
		$this->schemaManager->flushCache();

		// Ensure database name is set
		if(empty($parameters['database'])) {
			$parameters['database'] = 'database' . self::database_extension();
		}
		$dbName = $parameters['database'];
		if(!self::is_valid_database_name($dbName)) {
			// If not using the correct file extension for database files then the
			// results of SQLite3SchemaManager::databaseList will be unpredictable
			$extension = self::database_extension();
			Deprecation::notice('3.2', "SQLite3Database now expects a database file with extension \"$extension\". Behaviour may be unpredictable otherwise.");
		}

		// use the very lightspeed SQLite In-Memory feature for testing
		if($this->getLivesInMemory()) {
			$file = ':memory:';
		} else {
			// Ensure path is given
			if(empty($parameters['path'])) {
				$parameters['path'] = ASSETS_PATH . '/.sqlitedb';
			}

			//assumes that the path to dbname will always be provided:
			$file = $parameters['path'] . '/' . $dbName;
			if(!file_exists($parameters['path'])) {
				SQLiteDatabaseConfigurationHelper::create_db_dir($parameters['path']);
				SQLiteDatabaseConfigurationHelper::secure_db_dir($parameters['path']);
			}
		}
		
		// 'path' and 'database' are merged into the full file path, which
		// is the format that connectors such as PDOConnector expect
		$parameters['filepath'] = $file;

		// Ensure that driver is available (required by PDO)
		if(empty($parameters['driver'])) {
			$parameters['driver'] = $this->getDatabaseServer();
		}

		$this->connector->connect($parameters, true);

		foreach(self::$default_pragma as $pragma => $value) {
			$this->setPragma($pragma, $value);
		}

		if(empty(self::$default_pragma['locking_mode'])) {
			self::$default_pragma['locking_mode'] = $this->getPragma('locking_mode');
		}
	}

	/**
	 * Retrieve parameters used to connect to this SQLLite database
	 * 
	 * @return array
	 */
	public function getParameters() {
		return $this->parameters;
	}

	public function getLivesInMemory() {
		return isset($this->parameters['path']) && $this->parameters['path'] === ':memory:';
	}

	public function supportsCollations() {
		return true;
	}

	public function supportsTimezoneOverride() {
		return false;
	}

	/**
	 * Execute PRAGMA commands.
	 * 
	 * @param string pragma name
	 * @param string value to set
	 */
	public function setPragma($pragma, $value) {
		$this->query("PRAGMA $pragma = $value");
	}

	/**
	 * Gets pragma value.
	 * 
	 * @param string pragma name
	 * @return string the pragma value
	 */
	public function getPragma($pragma) {
		return $this->query("PRAGMA $pragma")->value();
	}

	public function getDatabaseServer() {
		return "sqlite";
	}

	public function selectDatabase($name, $create = false, $errorLevel = E_USER_ERROR) {
		if (!$this->schemaManager->databaseExists($name)) {
			// Check DB creation permisson
			if (!$create) {
				if ($errorLevel !== false) {
					user_error("Attempted to connect to non-existing database \"$name\"", $errorLevel);
				}
				// Unselect database
				$this->connector->unloadDatabase();
				return false;
			}
			$this->schemaManager->createDatabase($name);
		}

		// Reconnect using the existing parameters
		$parameters = $this->parameters;
		$parameters['database'] = $name;
		$this->connect($parameters);
		return true;
	}

	function now(){
		return "datetime('now', 'localtime')";
	}

	function random(){
		return 'random()';
	}

	/**
	 * The core search engine configuration.
	 * @todo There is a fulltext search for SQLite making use of virtual tables, the fts3 extension and the
	 * MATCH operator
	 * there are a few issues with fts:
	 * - shared cached lock doesn't allow to create virtual tables on versions prior to 3.6.17
	 * - there must not be more than one MATCH operator per statement
	 * - the fts3 extension needs to be available
	 * for now we use the MySQL implementation with the MATCH()AGAINST() uglily replaced with LIKE
	 * 
	 * @param string $keywords Keywords as a space separated string
	 * @return object DataObjectSet of result pages
	 */
	public function searchEngine($classesToSearch, $keywords, $start, $pageLength, $sortBy = "Relevance DESC",
		$extraFilter = "", $booleanSearch = false, $alternativeFileFilter = "", $invertedMatch = false
	) {
		$keywords = $this->escapeString(str_replace(array('*','+','-','"','\''), '', $keywords));
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
		$fields = $this->getSchemaManager()->fieldList('File');
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
			$match['File'] = "(Name LIKE '%$keywords%' OR Title LIKE '%$keywords%') AND ClassName = 'File'";

			// We make the relevance search by converting a boolean mode search into a normal one
			$relevanceKeywords = $keywords;
			$htmlEntityRelevanceKeywords = $htmlEntityKeywords;
			$relevance['SiteTree'] = "(Title LIKE '%$relevanceKeywords%' OR MenuTitle LIKE '%$relevanceKeywords%' OR Content LIKE '%$relevanceKeywords%' OR MetaDescription LIKE '%$relevanceKeywords%') + (Title LIKE '%$htmlEntityRelevanceKeywords%' OR MenuTitle LIKE '%$htmlEntityRelevanceKeywords%' OR Content LIKE '%$htmlEntityRelevanceKeywords%' OR MetaDescription LIKE '%$htmlEntityRelevanceKeywords%')";
			$relevance['File'] = "(Name LIKE '%$relevanceKeywords%' OR Title LIKE '%$relevanceKeywords%')";
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
				"NULL AS \"Content\"",
				"\"LastEdited\"",
				"\"Created\"",
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
		$queryParameters = array();
		$totalCount = 0;
		foreach($queries as $query) {
			$querySQLs[] = $query->sql($parameters);
			$queryParameters = array_merge($queryParameters, $parameters);
			$totalCount += $query->unlimitedRowCount();
		}

		$fullQuery = implode(" UNION ", $querySQLs) . " ORDER BY $sortBy LIMIT $limit";
		// Get records
		$records = $this->preparedQuery($fullQuery, $queryParameters);

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

	public function supportsExtensions($extensions = array('partitions', 'tablespaces', 'clustering')){

		if(isset($extensions['partitions']))
			return true;
		elseif(isset($extensions['tablespaces']))
			return true;
		elseif(isset($extensions['clustering']))
			return true;
		else
			return false;
	}

	public function transactionStart($transaction_mode = false, $session_characteristics = false) {
		$this->query('BEGIN');
	}

	public function transactionSavepoint($savepoint) {
		$this->query("SAVEPOINT \"$savepoint\"");
	}

	public function transactionRollback($savepoint = false){

		if($savepoint) {
			$this->query("ROLLBACK TO $savepoint;");
		} else {
			$this->query('ROLLBACK;');
		}
	}

	public function transactionEnd($chain = false){
		$this->query('COMMIT;');
	}

	public function clearTable($table) {
		$this->query("DELETE FROM \"$table\"");
	}

	public function comparisonClause($field, $value, $exact = false, $negate = false, $caseSensitive = null,
		$parameterised = false
	) {
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

		if($parameterised) {
			return sprintf("%s %s ?", $field, $comp);
		} else {
			return sprintf("%s %s '%s'", $field, $comp, $value);
		}
	}

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
