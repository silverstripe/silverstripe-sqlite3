<?php

// Called from DatabaseAdapterRegistry::autoconfigure($config)
use SilverStripe\Core\Environment;
use SilverStripe\SQLite\SQLite3Database;

if (!isset($databaseConfig)) {
    global $databaseConfig;
}

// Get path
$path = Environment::getEnv(SQLite3Database::ENV_PATH);
if ($path) {
    $databaseConfig['path'] = $path;
} elseif (defined(SQLite3Database::ENV_PATH)) {
    $databaseConfig['path'] = constant(SQLite3Database::ENV_PATH);
}

// Get key
$key = Environment::getEnv(SQLite3Database::ENV_KEY);
if ($key) {
    $databaseConfig['key'] = $key;
} elseif (defined(SQLite3Database::ENV_KEY)) {
    $databaseConfig['key'] = constant(SQLite3Database::ENV_KEY);
}
