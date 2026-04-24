<?php

namespace SilverStripe\SQLite;

use SilverStripe\ORM\Connect\DBConnector;
use SQLite3;

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
        try {
            $statement = @$this->dbConn->prepare($sql);
        } catch (\Throwable $e) {
            $values = $this->parameterValues($parameters);
            $this->throwRelevantError($e->getMessage(), $errorLevel, $sql, $values);
            return null;
        }

        if ($statement) {
            // Bind and run to statement
            for ($i = 0; $i < count($parsedParameters); $i++) {
                $value = $parsedParameters[$i]['value'];
                $type = $parsedParameters[$i]['type'];
                $statement->bindValue($i + 1, $value, $type);
            }

            // Return successful result
            try {
                $handle = $statement->execute();
            } catch (\Throwable $e) {
                $values = $this->parameterValues($parameters);
                $this->throwRelevantError($e->getMessage(), $errorLevel, $sql, $values);
                return null;
            }

            if ($handle) {
                return new SQLite3Query($this, $handle);
            }
        }

        // Handle error
        $values = $this->parameterValues($parameters);
        $this->throwRelevantError($this->getLastError(), $errorLevel, $sql, $values);
        return null;
    }

    public function query($sql, $errorLevel = E_USER_ERROR)
    {
        // Return successful result
        try {
            $handle = @$this->dbConn->query($sql);
        } catch (\Throwable $e) {
            $this->throwRelevantError($e->getMessage(), $errorLevel, $sql);
            return null;
        }

        if ($handle) {
            return new SQLite3Query($this, $handle);
        }

        // Handle error
        $this->throwRelevantError($this->getLastError(), $errorLevel, $sql);
        return null;
    }

    /**
     * Translate SQLite-specific errors into framework-aware exceptions where possible.
     *
     * @param string|null $message
     * @param int $errorLevel
     * @param string|null $sql
     * @param array $parameters
     */
    protected function throwRelevantError($message, $errorLevel, $sql = null, $parameters = [])
    {
        $isUniqueError = $errorLevel === E_USER_ERROR
            && is_string($message)
            && strpos($message, 'UNIQUE constraint failed: ') !== false;

        if ($isUniqueError) {
            preg_match('/UNIQUE constraint failed: (?P<fields>.+)$/', $message, $matches);

            $resolver = new SQLite3DuplicateEntryResolver($this->dbConn, [$this, 'parsePreparedParameters']);
            $resolved = $resolver->resolve(isset($matches['fields']) ? $matches['fields'] : '', $sql, $parameters);

            $this->duplicateEntryError(
                $message,
                isset($resolved['key']) ? $resolved['key'] : null,
                isset($resolved['value']) ? $resolved['value'] : null,
                $sql,
                $parameters
            );
            return;
        }

        $this->databaseError($message, $errorLevel, $sql, $parameters);
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
}
