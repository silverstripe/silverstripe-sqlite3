<?php

namespace SilverStripe\SQLite;

use Exception;
use SilverStripe\Control\Director;
use SilverStripe\Dev\Debug;
use SilverStripe\ORM\Connect\DBSchemaManager;
use SQLite3;

/**
 * SQLite schema manager class
 */
class SQLite3SchemaManager extends DBSchemaManager
{

    /**
     * Instance of the database controller this schema belongs to
     *
     * @var SQLite3Database
     */
    protected $database = null;

    /**
     * Flag indicating whether or not the database has been checked and repaired
     *
     * @var boolean
     */
    protected static $checked_and_repaired = false;

    /**
     * Should schema be vacuumed during checkeAndRepairTable?
     *
     * @var boolean
     */
    public static $vacuum = true;

    public function createDatabase($name)
    {
        // Ensure that any existing database is cleared before connection
        $this->dropDatabase($name);
    }

    public function dropDatabase($name)
    {
        // No need to delete database files if operating purely within memory
        if ($this->database->getLivesInMemory()) {
            return;
        }

        // If using file based database ensure any existing file is removed
        $path = $this->database->getPath();
        $fullpath = $path . '/' . $name . SQLite3Database::database_extension();
        if (is_writable($fullpath)) {
            unlink($fullpath);
        }
    }

    public function databaseList()
    {
        // If in-memory use the current database name only
        if ($this->database->getLivesInMemory()) {
            return array(
                $this->database->getConnector()->getSelectedDatabase()
                    ?: 'database'
            );
        }

        // If using file based database enumerate files in the database directory
        $directory = $this->database->getPath();
        $files = scandir($directory);

        // Filter each file in this directory
        $databases = array();
        if ($files !== false) {
            foreach ($files as $file) {
                // Filter non-files
                if (!is_file("$directory/$file")) {
                    continue;
                }

                // Filter those with correct extension
                if (!SQLite3Database::is_valid_database_name($file)) {
                    continue;
                }

                if ($extension = SQLite3Database::database_extension()) {
                    $databases[] = substr($file, 0, -strlen($extension));
                } else {
                    $databases[] = $file;
                }
            }
        }
        return $databases;
    }

    public function databaseExists($name)
    {
        $databases = $this->databaseList();
        return in_array($name, $databases);
    }

    /**
     * Empties any cached enum values
     */
    public function flushCache()
    {
        $this->enum_map = array();
    }

    public function schemaUpdate($callback)
    {
        // Set locking mode
        $this->database->setPragma('locking_mode', 'EXCLUSIVE');
        $this->checkAndRepairTable();
        $this->flushCache();

        // Initiate schema update
        $error = null;
        try {
            parent::schemaUpdate($callback);
        } catch (Exception $ex) {
            $error = $ex;
        }

        // Revert locking mode
        $this->database->setPragma('locking_mode', SQLite3Database::$default_pragma['locking_mode']);

        if ($error) {
            throw $error;
        }
    }

    /**
     * Empty a specific table
     *
     * @param string $table
     */
    public function clearTable($table)
    {
        if ($table != 'SQLiteEnums') {
            $this->query("DELETE FROM \"$table\"");
        }
    }

    public function createTable($table, $fields = null, $indexes = null, $options = null, $advancedOptions = null)
    {
        if (!isset($fields['ID'])) {
            $fields['ID'] = $this->IdColumn();
        }

        $fieldSchemata = array();
        if ($fields) {
            foreach ($fields as $k => $v) {
                $fieldSchemata[] = "\"$k\" $v";
            }
        }
        $fieldSchemas = implode(",\n", $fieldSchemata);

        // Switch to "CREATE TEMPORARY TABLE" for temporary tables
        $temporary = empty($options['temporary']) ? "" : "TEMPORARY";
        $this->query("CREATE $temporary TABLE \"$table\" (
			$fieldSchemas
		)");

        if ($indexes) {
            foreach ($indexes as $indexName => $indexDetails) {
                $this->createIndex($table, $indexName, $indexDetails);
            }
        }

        return $table;
    }

    public function alterTable(
        $tableName,
        $newFields = null,
        $newIndexes = null,
        $alteredFields = null,
        $alteredIndexes = null,
        $alteredOptions = null,
        $advancedOptions = null
    ) {
        if ($newFields) {
            foreach ($newFields as $fieldName => $fieldSpec) {
                $this->createField($tableName, $fieldName, $fieldSpec);
            }
        }

        if ($alteredFields) {
            foreach ($alteredFields as $fieldName => $fieldSpec) {
                $this->alterField($tableName, $fieldName, $fieldSpec);
            }
        }

        if ($newIndexes) {
            foreach ($newIndexes as $indexName => $indexSpec) {
                $this->createIndex($tableName, $indexName, $indexSpec);
            }
        }

        if ($alteredIndexes) {
            foreach ($alteredIndexes as $indexName => $indexSpec) {
                $this->alterIndex($tableName, $indexName, $indexSpec);
            }
        }
    }

    public function renameTable($oldTableName, $newTableName)
    {
        $this->query("ALTER TABLE \"$oldTableName\" RENAME TO \"$newTableName\"");
    }

    public function checkAndRepairTable($tableName = null)
    {
        $ok = true;

        if (!self::$checked_and_repaired) {
            $this->alterationMessage("Checking database integrity", "repaired");

            // Check for any tables with failed integrity
            if ($messages = $this->query('PRAGMA integrity_check')) {
                foreach ($messages as $message) {
                    if ($message['integrity_check'] != 'ok') {
                        Debug::show($message['integrity_check']);
                        $ok = false;
                    }
                }
            }

            // If enabled vacuum (clean and rebuild) the database
            if (self::$vacuum) {
                $this->query('VACUUM', E_USER_NOTICE);
                $message = $this->database->getConnector()->getLastError();
                if (preg_match('/authoriz/', $message ?? '')) {
                    $this->alterationMessage("VACUUM | $message", "error");
                } else {
                    $this->alterationMessage("VACUUMing", "repaired");
                }
            }
            self::$checked_and_repaired = true;
        }

        return $ok;
    }

    public function createField($table, $field, $spec)
    {
        $this->query("ALTER TABLE \"$table\" ADD \"$field\" $spec");
    }

    /**
     * Change the database type of the given field.
     * @param string $tableName The name of the tbale the field is in.
     * @param string $fieldName The name of the field to change.
     * @param string $fieldSpec The new field specification
     */
    public function alterField($tableName, $fieldName, $fieldSpec)
    {
        $oldFieldList = $this->fieldList($tableName);
        $fieldNameList = '"' . implode('","', array_keys($oldFieldList)) . '"';

        if (!empty($_REQUEST['avoidConflict']) && Director::isDev()) {
            $fieldSpec = preg_replace('/\snot null\s/i', ' NOT NULL ON CONFLICT REPLACE ', $fieldSpec);
        }

        // Skip non-existing columns
        if (!array_key_exists($fieldName, $oldFieldList)) {
            return;
        }

        // Update field spec
        $newColsSpec = array();
        foreach ($oldFieldList as $name => $oldSpec) {
            $newColsSpec[] = "\"$name\" " . ($name == $fieldName ? $fieldSpec : $oldSpec);
        }

        $queries = array(
            "CREATE TABLE \"{$tableName}_alterfield_{$fieldName}\"(" . implode(',', $newColsSpec) . ")",
            "INSERT INTO \"{$tableName}_alterfield_{$fieldName}\" SELECT {$fieldNameList} FROM \"$tableName\"",
            "DROP TABLE \"$tableName\"",
            "ALTER TABLE \"{$tableName}_alterfield_{$fieldName}\" RENAME TO \"$tableName\"",
        );

        // Remember original indexes
        $indexList = $this->indexList($tableName);

        // Then alter the table column
        $database = $this->database;
        $database->withTransaction(function () use ($database, $queries, $indexList) {
            foreach ($queries as $query) {
                $database->query($query . ';');
            }
        });

        // Recreate the indexes
        foreach ($indexList as $indexName => $indexSpec) {
            $this->createIndex($tableName, $indexName, $indexSpec);
        }
    }

    public function renameField($tableName, $oldName, $newName)
    {
        $oldFieldList = $this->fieldList($tableName);

        // Skip non-existing columns
        if (!array_key_exists($oldName, $oldFieldList)) {
            return;
        }

        // Determine column mappings
        $oldCols = array();
        $newColsSpec = array();
        foreach ($oldFieldList as $name => $spec) {
            $oldCols[] = "\"$name\"" . (($name == $oldName) ? " AS $newName" : '');
            $newColsSpec[] = "\"" . (($name == $oldName) ? $newName : $name) . "\" $spec";
        }

        // SQLite doesn't support direct renames through ALTER TABLE
        $oldColsStr = implode(',', $oldCols);
        $newColsSpecStr = implode(',', $newColsSpec);
        $queries = array(
            "CREATE TABLE \"{$tableName}_renamefield_{$oldName}\" ({$newColsSpecStr})",
            "INSERT INTO \"{$tableName}_renamefield_{$oldName}\" SELECT {$oldColsStr} FROM \"$tableName\"",
            "DROP TABLE \"$tableName\"",
            "ALTER TABLE \"{$tableName}_renamefield_{$oldName}\" RENAME TO \"$tableName\"",
        );

        // Remember original indexes
        $oldIndexList = $this->indexList($tableName);

        // Then alter the table column
        $database = $this->database;
        $database->withTransaction(function () use ($database, $queries) {
            foreach ($queries as $query) {
                $database->query($query . ';');
            }
        });

        // Recreate the indexes
        foreach ($oldIndexList as $indexName => $indexSpec) {
            // Map index columns
            $columns = array_filter(array_map(function ($column) use ($newName, $oldName) {
                // Unchanged
                if ($column !== $oldName) {
                    return $column;
                }
                // Skip obsolete fields
                if (stripos($newName, '_obsolete_') === 0) {
                    return null;
                }
                return $newName;
            }, $indexSpec['columns']));

            // Create index if column count unchanged
            if (count($columns) === count($indexSpec['columns'])) {
                $indexSpec['columns'] = $columns;
                $this->createIndex($tableName, $indexName, $indexSpec);
            }
        }
    }

    public function fieldList($table)
    {
        $sqlCreate = $this->preparedQuery(
            'SELECT "sql" FROM "sqlite_master" WHERE "type" = ? AND "name" = ?',
            array('table', $table)
        )->record();

        $fieldList = array();
        if ($sqlCreate && $sqlCreate['sql']) {
            preg_match(
                '/^[\s]*CREATE[\s]+TABLE[\s]+[\'"]?[a-zA-Z0-9_\\\]+[\'"]?[\s]*\((.+)\)[\s]*$/ims',
                $sqlCreate['sql'] ?? '',
                $matches
            );
            $fields = isset($matches[1])
                ? preg_split('/,(?=(?:[^\'"]*$)|(?:[^\'"]*[\'"][^\'"]*[\'"][^\'"]*)*$)/x', $matches[1])
                : array();
            foreach ($fields as $field) {
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
     *
     * @param string $tableName The name of the table.
     * @param string $indexName The name of the index.
     * @param array $indexSpec The specification of the index, see Database::requireIndex() for more details.
     */
    public function createIndex($tableName, $indexName, $indexSpec)
    {
        $sqliteName = $this->buildSQLiteIndexName($tableName, $indexName);
        $columns = $this->implodeColumnList($indexSpec['columns']);
        $unique = ($indexSpec['type'] == 'unique') ? 'UNIQUE' : '';
        $this->query("CREATE $unique INDEX IF NOT EXISTS \"$sqliteName\" ON \"$tableName\" ($columns)");
    }

    public function alterIndex($tableName, $indexName, $indexSpec)
    {
        // Drop existing index
        $sqliteName = $this->buildSQLiteIndexName($tableName, $indexName);
        $this->query("DROP INDEX IF EXISTS \"$sqliteName\"");

        // Create the index
        $this->createIndex($tableName, $indexName, $indexSpec);
    }

    /**
     * Builds the internal SQLLite index name given the silverstripe table and index name.
     *
     * The name is built using the table and index name in order to prevent name collisions
     * between indexes of the same name across multiple tables
     *
     * @param string $tableName
     * @param string $indexName
     * @return string The SQLite3 name of the index
     */
    protected function buildSQLiteIndexName($tableName, $indexName)
    {
        return "{$tableName}_{$indexName}";
    }

    public function indexKey($table, $index, $spec)
    {
        return $this->buildSQLiteIndexName($table, $index);
    }

    protected function convertIndexSpec($indexSpec)
    {
        $supportedIndexTypes = ['index', 'unique'];
        if (isset($indexSpec['type']) && !in_array($indexSpec['type'], $supportedIndexTypes)) {
            $indexSpec['type'] = 'index';
        }
        return parent::convertIndexSpec($indexSpec);
    }

    public function indexList($table)
    {
        $indexList = array();

        // Enumerate each index and related fields
        foreach ($this->query("PRAGMA index_list(\"$table\")") as $index) {
            // The SQLite internal index name, not the actual Silverstripe name
            $indexName = $index["name"];
            $indexType = $index['unique'] ? 'unique' : 'index';

            // Determine a clean list of column names within this index
            $list = array();
            foreach ($this->query("PRAGMA index_info(\"$indexName\")") as $details) {
                $list[] = preg_replace('/^"?(.*)"?$/', '$1', $details['name']);
            }

            // Safely encode this spec
            $indexList[$indexName] = array(
                'name' => $indexName,
                'columns' => $list,
                'type' => $indexType,
            );
        }

        return $indexList;
    }

    public function tableList()
    {
        $tables = array();
        $result = $this->preparedQuery('SELECT name FROM sqlite_master WHERE type = ?', array('table'));
        foreach ($result as $record) {
            $table = reset($record);
            $tables[strtolower($table)] = $table;
        }
        return $tables;
    }

    /**
     * Return a boolean type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function boolean($values)
    {
        $default = empty($values['default']) ? 0 : (int)$values['default'];
        return "BOOL NOT NULL DEFAULT $default";
    }

    /**
     * Return a date type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function date($values)
    {
        return "TEXT";
    }

    /**
     * Return a decimal type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function decimal($values)
    {
        $default = isset($values['default']) && is_numeric($values['default']) ? $values['default'] : 0;
        return "NUMERIC NOT NULL DEFAULT $default";
    }

    /**
     * Cached list of enum values indexed by table.column
     *
     * @var array
     */
    protected $enum_map = array();

    /**
     * Return a enum type-formatted string
     *
     * enums are not supported. as a workaround to store allowed values we creates an additional table
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function enum($values)
    {
        $tablefield = $values['table'] . '.' . $values['name'];
        $enumValues = implode(',', $values['enums']);

        // Ensure the cache table exists
        if (empty($this->enum_map)) {
            $this->query(
                "CREATE TABLE IF NOT EXISTS \"SQLiteEnums\" (\"TableColumn\" TEXT PRIMARY KEY, \"EnumList\" TEXT)"
            );
        }

        // Ensure the table row exists
        if (empty($this->enum_map[$tablefield]) || $this->enum_map[$tablefield] != $enumValues) {
            $this->preparedQuery(
                "REPLACE INTO SQLiteEnums (TableColumn, EnumList) VALUES (?, ?)",
                array($tablefield, $enumValues)
            );
            $this->enum_map[$tablefield] = $enumValues;
        }

        // Set default
        if (!empty($values['default'])) {
            /*
            On escaping strings:

            https://www.sqlite.org/lang_expr.html
            "A string constant is formed by enclosing the string in single quotes ('). A single quote within
            the string can be encoded by putting two single quotes in a row - as in Pascal. C-style escapes
            using the backslash character are not supported because they are not standard SQL."

            Also, there is a nifty PHP function for this. However apparently one must still be cautious of
            the null character ('\0' or 0x0), as per https://bugs.php.net/bug.php?id=63419
            */
            $default = SQLite3::escapeString(str_replace("\0", "", $values['default']));
            return "TEXT DEFAULT '$default'";
        } else {
            return 'TEXT';
        }
    }

    /**
     * Return a set type-formatted string
     * This type doesn't exist in SQLite either
     *
     * @see SQLite3SchemaManager::enum()
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function set($values)
    {
        return $this->enum($values);
    }

    /**
     * Return a float type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function float($values)
    {
        return "REAL";
    }

    /**
     * Return a Double type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function double($values)
    {
        return "REAL";
    }

    /**
     * Return a int type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function int($values)
    {
        return "INTEGER({$values['precision']}) " . strtoupper($values['null']) . " DEFAULT " . (int)$values['default'];
    }

    /**
     * Return a bigint type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function bigint($values)
    {
        return $this->int($values);
    }

    /**
     * Return a datetime type-formatted string
     * For SQLite3, we simply return the word 'TEXT', no other parameters are necessary
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function datetime($values)
    {
        return "DATETIME";
    }

    /**
     * Return a text type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function text($values)
    {
        return 'TEXT';
    }

    /**
     * Return a time type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function time($values)
    {
        return "TEXT";
    }

    /**
     * Return a varchar type-formatted string
     *
     * @param array $values Contains a tokenised list of info about this data type
     * @return string
     */
    public function varchar($values)
    {
        return "VARCHAR({$values['precision']}) COLLATE NOCASE";
    }

    /*
     * Return a 4 digit numeric type.  MySQL has a proprietary 'Year' type.
     * For SQLite3 we use TEXT
     */
    public function year($values, $asDbValue = false)
    {
        return "TEXT";
    }

    public function IdColumn($asDbValue = false, $hasAutoIncPK = true)
    {
        return 'INTEGER PRIMARY KEY AUTOINCREMENT';
    }

    public function hasTable($tableName)
    {
        return (bool)$this->preparedQuery(
            'SELECT "name" FROM "sqlite_master" WHERE "type" = ? AND "name" = ?',
            array('table', $tableName)
        )->first();
    }

    /**
     * Return enum values for the given field
     *
     * @param string $tableName
     * @param string $fieldName
     * @return array
     */
    public function enumValuesForField($tableName, $fieldName)
    {
        $tablefield = "$tableName.$fieldName";

        // Check already cached values for this field
        if (!empty($this->enum_map[$tablefield])) {
            return explode(',', $this->enum_map[$tablefield]);
        }

        // Retrieve and cache these details from the database
        $classnameinfo = $this->preparedQuery(
            "SELECT EnumList FROM SQLiteEnums WHERE TableColumn = ?",
            array($tablefield)
        )->first();
        if ($classnameinfo) {
            $valueList = $classnameinfo['EnumList'];
            $this->enum_map[$tablefield] = $valueList;
            return explode(',', $valueList);
        }

        // Fallback to empty list
        return array();
    }

    public function dbDataType($type)
    {
        $values = array(
            'unsigned integer' => 'INT'
        );

        if (isset($values[$type])) {
            return $values[$type];
        } else {
            return '';
        }
    }
}
