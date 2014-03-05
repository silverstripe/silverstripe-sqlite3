<?php

Deprecation::notification_version('1.3.0', 'sqlite3');

$classes = array('SQLiteDatabase', 'SQLite3Database', 'SQLitePDODatabase');

global $databaseConfig;
if(defined('SS_DATABASE_CLASS') && in_array(SS_DATABASE_CLASS, $classes)) {
	$databaseConfig['type'] = SS_DATABASE_CLASS;
}

if(in_array($databaseConfig['type'], $classes)) {
	if(empty($databaseConfig['path'])) {
		// default the database path to a location in assets
		$databaseConfig['path'] = defined('SS_SQLITE_DATABASE_PATH') && SS_SQLITE_DATABASE_PATH ? SS_SQLITE_DATABASE_PATH : ASSETS_PATH . '/.sqlitedb/';
	}

	$databaseConfig['database'] = (defined('SS_DATABASE_PREFIX') ? SS_DATABASE_PREFIX : '') . $databaseConfig['database'] . (defined('SS_DATABASE_SUFFIX') ? SS_DATABASE_SUFFIX : '');

	if(empty($databaseConfig['key'])) {
		$databaseConfig['key'] = defined('SS_SQLITE_DATABASE_KEY')  && SS_SQLITE_DATABASE_KEY  ? SS_SQLITE_DATABASE_KEY :  'SQLite3DatabaseKey';
	}

	/**
	 * set pragma values on the connection.
	 * @see http://www.sqlite.org/pragma.html
	 */
	SQLite3Database::$default_pragma['encoding'] = '"UTF-8"';
	SQLite3Database::$default_pragma['locking_mode'] = 'NORMAL';

	// The SQLite3 class is available in PHP 5.3 and newer
	if(class_exists('SQLite3') && $databaseConfig['type'] != 'SQLitePDODatabase') {
		$databaseConfig['type'] = 'SQLite3Database';
	} else {
		$databaseConfig['type'] = 'SQLitePDODatabase';
	}
}
