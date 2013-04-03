<?php

/**
 * This is a helper class for the SS installer.
 * 
 * It does all the specific checking for SQLiteDatabase
 * to ensure that the configuration is setup correctly.
 * 
 * @package SQLite3
 */
class SQLiteDatabaseConfigurationHelper implements DatabaseConfigurationHelper {
	
	/**
	 * Create a connection of the appropriate type
	 * 
	 * @param array $databaseConfig
	 * @param string $error Error message passed by value
	 * @return mixed|null Either the connection object, or null if error
	 */
	protected function createConnection($databaseConfig, &$error) {
		$error = null;
		try {
			if(!file_exists($databaseConfig['path'])) {
				self::create_db_dir($databaseConfig['path']);
				self::secure_db_dir($databaseConfig['path']);
			}
			$file = $databaseConfig['path'] . '/' . $databaseConfig['database'];
			$conn = null;
		
			switch($databaseConfig['type']) {
				case 'SQLite3Database':
					if(empty($databaseConfig['key'])) {
						$conn = @new SQLite3($file, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE);
					} else {
						$conn = @new SQLite3($file, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, $databaseConfig['key']);
					}
					break;
				case 'SQLite3PDODatabase':
					// May throw a PDOException if fails
					$conn = @new PDO("sqlite:$file");
					break;
				default:
					$error = 'Invalid connection type';
					return null;
			}
			
			if($conn) {
				return $conn;
			} else {
				$error = 'Unknown connection error';
				return null;
			}
		} catch(Exception $ex) {
			$error = $ex->getMessage();
			return null;
		}
	}
	
	public function requireDatabaseFunctions($databaseConfig) {
		$data = DatabaseAdapterRegistry::get_adapter($databaseConfig['type']);
		return !empty($data['supported']);
	}

	public function requireDatabaseServer($databaseConfig) {
		$path = $databaseConfig['path'];
		$error = '';
		$success = false;

		if(!$path) {
			$error = 'No database path provided';
		} elseif(is_writable($path) || (!file_exists($path) && is_writable(dirname($path)))) {
			// check if folder is writeable
			$success = true;
		} else {
			$error = "Permission denied";
		}

		return array(
			'success' => $success,
			'error' => $error,
			'path' => $path
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
		// Do additional validation around file paths
		if(empty($databaseConfig['path'])) return array(
			'success' => false,
			'error' => "Missing directory path"
		);
		if(empty($databaseConfig['database'])) return array(
			'success' => false,
			'error' => "Missing database filename"
		);
		
		// Create and secure db directory
		$path = $databaseConfig['path'];
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

		$conn = $this->createConnection($databaseConfig, $error);
		$success = !empty($conn);
		
		return array(
			'success' => $success,
			'connection' => $conn,
			'error' => $error
		);
	}

	public function getDatabaseVersion($databaseConfig) {
		$version = 0;
		
		switch($databaseConfig['type']) {
			case 'SQLite3Database':
				$info = SQLite3::version();
				$version = trim($info['versionString']);
				break;
			case 'SQLite3PDODatabase':
				// Fallback to using sqlite_version() query
				$conn = $this->createConnection($databaseConfig, $error);
				if($conn) {
					$version = $conn->getAttribute(PDO::ATTR_SERVER_VERSION);
				}
				break;
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

	public function requireDatabaseOrCreatePermissions($databaseConfig) {
		$conn = $this->createConnection($databaseConfig, $error);
		$success = $alreadyExists = !empty($conn);
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
		return file_exists($path) || mkdir($path);
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
	
	public function requireDatabaseAlterPermissions($databaseConfig) {
		// no concept of table-specific permissions; If you can connect you can alter schema
		return array(
			'success' => true,
			'applies' => false
		);
	}
}
