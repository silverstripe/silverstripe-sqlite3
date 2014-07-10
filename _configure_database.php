<?php

// Script called from ConfigureFromEnv.php
global $databaseConfig;
if(strpos($databaseConfig['type'], 'SQLite') === 0) {

	if(defined('SS_SQLITE_DATABASE_PATH')) {
		$databaseConfig['path'] = SS_SQLITE_DATABASE_PATH;
	}

	if(defined('SS_SQLITE_DATABASE_KEY')) {
		$databaseConfig['key'] = SS_SQLITE_DATABASE_KEY;
	}
}
