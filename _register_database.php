<?php

use SilverStripe\Dev\Install\DatabaseAdapterRegistry;
use SilverStripe\SQLite\SQLiteDatabaseConfigurationHelper;

$sqliteDatabaseAdapterRegistryFields = array(
    'path' => array(
        'title' => 'Directory path<br /><small>Absolute path to directory, writeable by the webserver user.<br />'
            . 'Recommended to be outside of your webroot</small>',
        'default' => dirname(dirname(__FILE__)) . DIRECTORY_SEPARATOR . 'assets' . DIRECTORY_SEPARATOR . '.sqlitedb'
    ),
    'database' => array(
        'title' => 'Database filename (extension .sqlite)',
        'default' => 'database.sqlite'
    )
);

// Basic SQLLite3 Database
/** @skipUpgrade */
DatabaseAdapterRegistry::register(
    array(
        'class' => 'SQLite3Database',
        'module' => 'sqlite3',
        'title' => 'SQLite 3.3+ (using SQLite3)',
        'helperPath' => __DIR__.'/code/SQLiteDatabaseConfigurationHelper.php',
        'helperClass' => SQLiteDatabaseConfigurationHelper::class,
        'supported' => class_exists('SQLite3'),
        'missingExtensionText' => 'The <a href="http://php.net/manual/en/book.sqlite3.php">SQLite3</a> 
            PHP Extension is not available. Please install or enable it of them and refresh this page.',
        'fields' => array_merge($sqliteDatabaseAdapterRegistryFields, array('key' => array(
            'title' => 'Encryption key<br><small>This function is experimental and requires configuration of an '
            . 'encryption module</small>',
            'default' => ''
        )))
    )
);

// PDO database
/** @skipUpgrade */
DatabaseAdapterRegistry::register(
    array(
        'class' => 'SQLite3PDODatabase',
        'module' => 'sqlite3',
        'title' => 'SQLite 3.3+ (using PDO)',
        'helperPath' => __DIR__.'/code/SQLiteDatabaseConfigurationHelper.php',
        'helperClass' => SQLiteDatabaseConfigurationHelper::class,
        'supported' => (class_exists('PDO') && in_array('sqlite', PDO::getAvailableDrivers())),
        'missingExtensionText' =>
            'Either the <a href="http://php.net/manual/en/book.pdo.php">PDO Extension</a> or the
            <a href="http://php.net/manual/en/book.sqlite3.php">SQLite3 PDO Driver</a>
            are unavailable. Please install or enable these and refresh this page.',
        'fields' => $sqliteDatabaseAdapterRegistryFields
    )
);
