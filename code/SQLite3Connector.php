<?php

namespace SilverStripe\SQLite;

use SilverStripe\Core\Environment;
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

    /**
     * Log file handle
     *
     * @var resource|null
     */
    protected $logHandle = null;

    /**
     * Check if error logging is enabled
     *
     * @return bool
     */
    protected function isErrorLoggingEnabled(): bool
    {
        $envValue = getenv('SS_SQLITE_LOG_ERRORS');
        if ($envValue === false) {
            $envValue = Environment::getEnv('SS_SQLITE_LOG_ERRORS');
        }
        return $envValue === 'true' || $envValue === '1' || $envValue === true;
    }

    /**
     * Get the log file path
     *
     * @return string
     */
    protected function getLogPath(): string
    {
        $path = getenv('SS_SQLITE_LOG_PATH');
        if ($path === false) {
            $path = Environment::getEnv('SS_SQLITE_LOG_PATH');
        }
        if ($path) {
            return $path;
        }
        // Default to project root
        return $this->findProjectRoot() . '/sqlite3_queries.log';
    }

    /**
     * Find the project root directory
     *
     * @return string
     */
    protected function findProjectRoot(): string
    {
        // Start from current file and traverse up to find composer.json
        $dir = __DIR__;
        while ($dir !== dirname($dir)) {
            if (file_exists($dir . '/composer.json')) {
                return $dir;
            }
            $dir = dirname($dir);
        }
        // Fallback to current directory
        return __DIR__;
    }

    /**
     * Log a SQL error for debugging
     *
     * @param string $sql The SQL statement that failed
     * @param array $parameters Optional parameters for prepared queries
     * @param float|null $duration Query execution time in milliseconds
     * @param string $error Error message
     */
    protected function logError(string $sql, array $parameters = [], ?float $duration = null, string $error = ''): void
    {
        if (!$this->isErrorLoggingEnabled()) {
            return;
        }

        $logPath = $this->getLogPath();
        $timestamp = date('Y-m-d H:i:s.u');

        // Format the log entry
        $entry = "[$timestamp]\n";
        $entry .= "SQL: $sql\n";

        if (!empty($parameters)) {
            $entry .= "PARAMS: " . json_encode($parameters) . "\n";
        }

        if ($duration !== null) {
            $entry .= "TIME: " . round($duration, 3) . "ms\n";
        }

        $entry .= "ERROR: $error\n";
        $entry .= str_repeat('-', 80) . "\n";

        // Write to log file
        file_put_contents($logPath, $entry, FILE_APPEND | LOCK_EX);
    }

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
        $startTime = microtime(true);
        $statement = null;

        // Type check, identify, and prepare parameters for passing to the statement bind function
        $parsedParameters = $this->parsePreparedParameters($parameters);

        // Prepare statement - need try/catch because enableExceptions(true) is set
        try {
            $statement = $this->dbConn->prepare($sql);
        } catch (Exception $e) {
            $duration = (microtime(true) - $startTime) * 1000;
            $this->logError($sql, $parameters, $duration, "PREPARE FAILED: " . $e->getMessage());
            $this->throwRelevantError($e->getMessage(), intval($e->getCode()), $errorLevel, $sql, $parameters);
            return null;
        }

        if (!$statement) {
            // Log failed prepare (when exceptions are disabled)
            $duration = (microtime(true) - $startTime) * 1000;
            $error = $this->getLastError();
            $this->logError($sql, $parameters, $duration, "PREPARE FAILED: " . $error);
        }

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
                $duration = (microtime(true) - $startTime) * 1000;
                $this->logError($sql, $parameters, $duration, $e->getMessage());
                $this->throwRelevantError($e->getMessage(), intval($e->getCode()), $errorLevel, $sql, $parameters);
            }
        }

        // Handle error when execute returns false but no exception
        $duration = (microtime(true) - $startTime) * 1000;
        $error = $this->getLastError();
        $this->logError($sql, $parameters, $duration, $error);
        $values = $this->parameterValues($parameters);
        $this->throwRelevantError($error, $this->getLastErrorCode(), $errorLevel, $sql, $values);

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
        $error = $this->getLastError();
        $this->logError($sql, [], null, $error);
        $this->throwRelevantError($error, $this->getLastErrorCode(), $errorLevel, $sql, []);
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
    private function throwRelevantError(
        string $message,
        int $code,
        int $errorLevel,
        ?string $sql,
        array $parameters
    ): void {
        if (
            $errorLevel === E_USER_ERROR
            && preg_match('/cannot\s+(?:UPDATE|INSERT\s+INTO)\s+generated\s+column\s+"(?P<column>[^"]+)"/i', $message, $matches)
        ) {
            $table = null;
            if ($sql && preg_match('/\b(?:UPDATE|INTO)\s+"?(?P<table>[A-Za-z0-9_]+)"?/i', $sql, $tableMatches)) {
                $table = $tableMatches['table'];
            }

            $this->valueForGeneratedColumnError($message, $matches['column'] ?? null, $table, $sql, $parameters);
        }

        // https://www.sqlite.org/rescode.html#constraint_unique
        if (
            $errorLevel === E_USER_ERROR && $code === 19
            && str_contains($message, "UNIQUE constraint failed")
        ) {
            // Could be one or more fields, eg: UNIQUE constraint failed:
            // DataObjectTest_UniqueIndexObject.Name, DataObjectTest_UniqueIndexObject.Code
            preg_match('/UNIQUE constraint failed: (?P<fields>[^\']+)?/', $message, $matches);

            $resolver = new SQLite3DuplicateEntryResolver($this->dbConn, [$this, 'parsePreparedParameters']);
            $resolved = $resolver->resolve($matches['fields'] ?? '', $sql, $parameters);

            $this->duplicateEntryError($message, $resolved['key'], $resolved['value'], $sql, $parameters);
        } else {
            $this->databaseError($message, $errorLevel, $sql, $parameters);
        }
    }
}
