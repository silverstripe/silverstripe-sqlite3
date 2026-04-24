<?php
/**
 * Bootstrap for SQLite3 module tests
 * Sets SQLite as the database and loads the framework bootstrap
 */

// Set SQLite as the database type
putenv('DB=SQLITE');
$_ENV['DB'] = 'SQLITE';

// Require the framework's bootstrap
require_once dirname(__DIR__) . '/vendor/silverstripe/framework/tests/bootstrap.php';
