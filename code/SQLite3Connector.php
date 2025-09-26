<?php

namespace SilverStripe\SQLite;

use SilverStripe\ORM\Connect\DBConnector;
use SQLite3;
use Exception;

/**
 * SQLite connector class
 */
class SQLite3Connector extends DBConnector
{
    /**
     * The name of the database.
     *
     * @var string
     */
    protected $databaseName;

    /**
     * Connection to the DBMS.
     *
     * @var SQLite3
     */
    protected $dbConn;

    public function connect($parameters, $selectDB = false)
    {
        $file = $parameters['filepath'];
        $this->dbConn = empty($parameters['key'])
            ? new SQLite3($file, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE)
            : new SQLite3($file, SQLITE3_OPEN_READWRITE | SQLITE3_OPEN_CREATE, $parameters['key']);
        $this->dbConn->busyTimeout(60000);
        $this->dbConn->enableExceptions(true);
        $this->databaseName = $parameters['database'];
    }

    public function affectedRows()
    {
        return $this->dbConn->changes();
    }

    public function getGeneratedID($table)
    {
        return $this->dbConn->lastInsertRowID();
    }

    public function getLastError()
    {
        $message = $this->dbConn->lastErrorMsg();
        return $message === 'not an error' ? null : $message;
    }

    public function getLastErrorCode(): int
    {
        return $this->dbConn->lastErrorCode();
    }

    public function getSelectedDatabase()
    {
        return $this->databaseName;
    }

    public function getVersion()
    {
        $version = SQLite3::version();
        return trim($version['versionString']);
    }

    public function isActive()
    {
        return $this->databaseName && $this->dbConn;
    }

    /**
     * Prepares the list of parameters in preparation for passing to mysqli_stmt_bind_param
     *
     * @param array $parameters List of parameters
     * @return array List of parameters types and values
     */
    public function parsePreparedParameters($parameters)
    {
        $values = array();
        foreach ($parameters as $value) {
            $phpType = gettype($value);
            $sqlType = null;

            // Allow overriding of parameter type using an associative array
            if ($phpType === 'array') {
                $phpType = $value['type'];
                $value = $value['value'];
            }

            // Convert php variable type to one that makes mysqli_stmt_bind_param happy
            // @see http://www.php.net/manual/en/mysqli-stmt.bind-param.php
            switch ($phpType) {
                case 'boolean':
                case 'integer':
                    $sqlType = SQLITE3_INTEGER;
                    break;
                case 'float': // Not actually returnable from gettype
                case 'double':
                    $sqlType = SQLITE3_FLOAT;
                    break;
                case 'object': // Allowed if the object or resource has a __toString method
                case 'resource':
                case 'string':
                    $sqlType = SQLITE3_TEXT;
                    break;
                case 'NULL':
                    $sqlType = SQLITE3_NULL;
                    break;
                case 'blob':
                    $sqlType = SQLITE3_BLOB;
                    break;
                case 'array':
                case 'unknown type':
                default:
                    $this->databaseError("Cannot bind parameter \"$value\" as it is an unsupported type ($phpType)");
                    break;
            }
            $values[] = array(
                'type' => $sqlType,
                'value' => $value
            );
        }
        return $values;
    }

    public function preparedQuery($sql, $parameters, $errorLevel = E_USER_ERROR)
    {
        // Type check, identify, and prepare parameters for passing to the statement bind function
        $parsedParameters = $this->parsePreparedParameters($parameters);

        // Prepare statement
        $statement = @$this->dbConn->prepare($sql);
        if ($statement) {
            // Bind and run to statement
            for ($i = 0; $i < count($parsedParameters); $i++) {
                $value = $parsedParameters[$i]['value'];
                $type = $parsedParameters[$i]['type'];
                $statement->bindValue($i + 1, $value, $type);
            }

            try {
                $handle = $statement->execute();
                // Return successful result
                if ($handle) {
                    return new SQLite3Query($this, $handle);
                }
            } catch (Exception $e) {
                $statement = false;
                $this->throwRelevantError($e->getMessage(), intval($e->getCode()), $errorLevel, $sql, $parameters);
            }
        }

        // Handle error
        $values = $this->parameterValues($parameters);
        $this->throwRelevantError($this->getLastError(), $this->getLastErrorCode(), $errorLevel, $sql, $values);

        return null;
    }

    public function query($sql, $errorLevel = E_USER_ERROR)
    {
        // Return successful result
        $handle = @$this->dbConn->query($sql);
        if ($handle) {
            return new SQLite3Query($this, $handle);
        }

        // Handle error
        $this->databaseError($this->getLastError(), $errorLevel, $sql);
        return null;
    }

    public function quoteString($value)
    {
        return "'" . $this->escapeString($value) . "'";
    }

    public function escapeString($value)
    {
        return $this->dbConn->escapeString($value ?? '');
    }

    public function selectDatabase($name)
    {
        if ($name !== $this->databaseName) {
            $this->databaseError("SQLite3Connector can't change databases. Please create a new database connection");
        }
        return true;
    }

    public function unloadDatabase()
    {
        $this->dbConn->close();
        $this->databaseName = null;
    }

    /**
     * Throw the correct DatabaseException for this error
     *
     * @throws DatabaseException
     */
    private function throwRelevantError(string $message, int $code, int $errorLevel, ?string $sql, array $parameters): void
    {
        // https://www.sqlite.org/rescode.html#constraint_unique
        if ($errorLevel === E_USER_ERROR && $code === 19 && str_contains($message, "UNIQUE constraint failed")) {
            // Could be one or more fields, eg: UNIQUE constraint failed: DataObjectTest_UniqueIndexObject.Name, DataObjectTest_UniqueIndexObject.Code
            preg_match('/UNIQUE constraint failed: (?P<fields>[^\']+)?/', $message, $matches);

            $matches = explode(",", $matches['fields'] ?? '');
            $fields = [];
            $table = null;
            foreach ($matches as $field) {
                $field = trim($field);

                // Remove table name from field
                if (str_contains($field, '.')) {
                    $parts = explode('.', $field);
                    $field = array_pop($parts);
                    $table = $parts[0] ?? $table;
                    $fields[] = $field;
                }
            }

            // Sqlite doesn't provide index name
            $key = implode(", ", $fields);

            // Sqlite doesn't provide value in error message
            $val = $parameters[1] ?? '';

            // HACK: comply with unit tests
            // if ($table === 'DataObjectTest_UniqueIndexObject') {
            //     // Single constraint takes precedence
            //     if (count($fields) > 1 && $val !== 'Same Value') {
            //         $key = 'MultiFieldIndex';
            //         $val = 'Same Value';
            //     } else {
            //         $key = 'SingleFieldIndex';
            //         $val = 'Same Value';
            //     }
            // }

            $this->duplicateEntryError($message, $key, (string)$val, $sql, $parameters);
        } else {
            $this->databaseError($message, $errorLevel, $sql, $parameters);
        }
    }
}
