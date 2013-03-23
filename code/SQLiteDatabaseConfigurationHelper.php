<?php
/**
 * This is a helper class for the SS installer.
 * 
 * It does all the specific checking for SQLiteDatabase
 * to ensure that the configuration is setup correctly.
 * 
 * @package sqlite3
 */
class SQLiteDatabaseConfigurationHelper implements DatabaseConfigurationHelper {

	/**
	 * Ensure that one of the database classes
	 * is available. If it is, we assume the PHP module for this
	 * database has been setup correctly.
	 * 
	 * @param array $databaseConfig Associative array of database configuration, e.g. "type", "path" etc
	 * @return boolean
	 */
	public function requireDatabaseFunctions($databaseConfig) {
		if($databaseConfig['type'] == 'SQLitePDODatabase' || version_compare(phpversion(), '5.3.0', '<')) return class_exists('PDO') ? true : false;
		return class_exists('SQLite3');
	}

	/**
	 * Ensure that the database server exists.
	 * @param array $databaseConfig Associative array of db configuration, e.g. "type", "path" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseServer($databaseConfig) {
		$path = $databaseConfig['path'];
		$error = '';

		if(!$path) {
			$success = false;
			$error = 'No database path provided';
		}
		// check if parent folder is writeable
		elseif(is_writable(dirname($path))) {
			$success = true;
		} else {
			$success = false;
			$error = 'Webserver can\'t write database file to path "' . $path . '"';
		}

		return array(
			'success' => $success,
			'error' => $error
		);
	}

	/**
	 * Ensure a database connection is possible using credentials provided.
	 * 
	 * @todo Validate path
	 * 
	 * @param array $databaseConfig Associative array of db configuration, e.g. "type", "path" etc
	 * @return array Result - e.g. array('success' => true, 'error' => 'details of error')
	 */
	public function requireDatabaseConnection($databaseConfig) {
		$success = false;
		$error = '';
		
		// arg validation
		if(!isset($databaseConfig['path']) || !$databaseConfig['path']) return array(
			'success' => false,
			'error' => sprintf('Invalid path: "%s"', $databaseConfig['path'])
		);
		$path = $databaseConfig['path'];
		
		if(!isset($databaseConfig['database']) || !$databaseConfig['database']) return array(
			'success' => false,
			'error' => sprintf('Invalid database name: "%s"', $databaseConfig['database'])
		);
		
		// create and secure db directory
		$dirCreated = self::create_db_dir($path);
		if(!$dirCreated) return array(
			'success' => false,
			'error' => sprintf('Cannot create path: "%s"', $path)
		);
		$dirSecured = self::secure_db_dir($path);
		if(!$dirSecured) return array(
			'success' => false,
			'error' => sprintf('Cannot secure path through .htaccess: "%s"', $path)
		);

		$file = $path . '/' . $databaseConfig['database'];
		$file = preg_replace('/\/$/', '', $file);

		if($databaseConfig['type'] == 'SQLitePDODatabase' || version_compare(phpversion(), '5.3.0', '<')) {
			$conn = @(new PDO("sqlite:$file"));
		} else {
			$conn = @(new SQLite3($file, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE));
		}

		if($conn) {
			$success = true;
		} else {
			$success = false;
			$error = '';
		}
		
		return array(
			'success' => $success,
			'connection' => $conn,
			'error' => $error
		);
	}

	public function getDatabaseVersion($databaseConfig) {
		$version = 0;

		if(class_exists('SQLite3')) {
			$info = SQLite3::version();
			if($info && isset($info['versionString'])) {
				$version = trim($info['versionString']);
			}
		} else {
			// Fallback to using sqlite_version() query
			$file = $databaseConfig['path'] . '/' . $databaseConfig['database'];
			$file = preg_replace('/\/$/', '', $file);
			$conn = @(new PDO("sqlite:$file"));
			if($conn) {
				$result = @$conn->query('SELECT sqlite_version()');
				$version = $result->fetchColumn();
			}
		}

		return $version;
	}

	public function requireDatabaseVersion($databaseConfig) {
		$success = false;
		$error = '';
		$version = $this->getDatabaseVersion($databaseConfig);

		if($version) {
			$success = version_compare($version, '3.3', '>=');
			if(!$success) {
				$error = "Your SQLite3 library version is $version. It's recommended you use at least 3.3.";
			}
		}

		return array(
			'success' => $success,
			'error' => $error
		);
	}

	/**
	 * Ensure that the database connection is able to use an existing database,
	 * or be able to create one if it doesn't exist.
	 *
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('success' => true, 'alreadyExists' => 'true')
	 */
	public function requireDatabaseOrCreatePermissions($databaseConfig) {
		$success = false;
		$alreadyExists = false;
		$canCreate = false;
		
		$check = $this->requireDatabaseConnection($databaseConfig);
		$conn = $check['connection'];
		
		if($conn) {
			$success = true;
			$alreadyExists = true;
		} else {
			$success = false;
			$alreadyExists = false;
		}

		return array(
			'success' => $success,
			'alreadyExists' => $alreadyExists,
		);
	}
	
	/**
	 * Creates the provided directory and prepares it for
	 * storing SQLlite. Use {@link secure_db_dir()} to
	 * secure it against unauthorized access.
	 * 
	 * @param String $path Absolute path, usually with a hidden folder.
	 * @return boolean
	 */
	public static function create_db_dir($path) {
		return (!file_exists($path)) ? mkdir($path) : true;
	}
	
	/**
	 * Secure the provided directory via web-access
	 * by placing a .htaccess file in it. 
	 * This is just required if the database directory
	 * is placed within a publically accessible webroot (the
	 * default path is in a hidden folder within assets/).
	 * 
	 * @param String $path Absolute path, containing a SQLite datatbase
	 * @return boolean
	 */
	public static function secure_db_dir($path) {
		return (is_writeable($path)) ? file_put_contents($path . '/.htaccess', 'deny from all') : false;
	}
	
	/**
	 * Ensure we have permissions to alter tables.
	 * 
	 * @param array $databaseConfig Associative array of db configuration, e.g. "server", "username" etc
	 * @return array Result - e.g. array('okay' => true, 'applies' => true), where applies is whether
	 * the test is relevant for the database
	 */
	public function requireDatabaseAlterPermissions($databaseConfig) {
		return array('success' => true, 'applies' => false);	
	}
}
