<?php

namespace SilverStripe\SQLite;

use SQLite3;

class SQLite3DuplicateEntryResolver
{
    /**
     * @var SQLite3
     */
    protected $connection;

    /**
     * @var callable
     */
    protected $parsePreparedParameters;

    /**
     * @param SQLite3 $connection
     * @param callable $parsePreparedParameters
     */
    public function __construct(SQLite3 $connection, callable $parsePreparedParameters)
    {
        $this->connection = $connection;
        $this->parsePreparedParameters = $parsePreparedParameters;
    }

    /**
     * @param string $constraintFields
     * @param string|null $sql
     * @param array $parameters
     * @return array
     */
    public function resolve($constraintFields, $sql, array $parameters)
    {
        list($table, $fields) = $this->parseUniqueConstraintFields($constraintFields);
        $key = $this->resolveDuplicateKeyName($table, $fields, $sql, $parameters);
        $value = $this->resolveDuplicatedValue($table, $key, $sql, $parameters);

        return array(
            'key' => $key,
            'value' => $value,
        );
    }

    /**
     * @param string $constraintFields
     * @return array
     */
    protected function parseUniqueConstraintFields($constraintFields)
    {
        $fields = array();
        $table = null;

        foreach (explode(',', $constraintFields) as $field) {
            $field = trim($field);
            if ($field === '') {
                continue;
            }

            if (strpos($field, '.') !== false) {
                $parts = explode('.', $field);
                $field = array_pop($parts);
                $table = isset($parts[0]) ? $parts[0] : $table;
            }

            $fields[] = $this->normaliseIdentifier($field);
        }

        return array($table, $fields);
    }

    /**
     * @param string|null $table
     * @param array $fields
     * @param string|null $sql
     * @param array $parameters
     * @return string|null
     */
    protected function resolveDuplicateKeyName($table, array $fields, $sql, array $parameters)
    {
        if (!$table) {
            return implode(', ', $fields);
        }

        $indexes = $this->getUniqueIndexes($table);
        $attemptedValues = $this->extractAttemptedValues($sql, $parameters);
        $violatedIndexes = array();

        foreach ($indexes as $indexName => $columns) {
            if ($this->isUniqueIndexViolated($table, $columns, $attemptedValues)) {
                $violatedIndexes[$indexName] = $columns;
            }
        }

        if ($violatedIndexes) {
            uasort($violatedIndexes, function (array $left, array $right) {
                return count($left) <=> count($right);
            });
            reset($violatedIndexes);
            return key($violatedIndexes);
        }

        foreach ($indexes as $indexName => $columns) {
            if ($columns === $fields) {
                return $indexName;
            }
        }

        return implode(', ', $fields);
    }

    /**
     * @param string|null $table
     * @param string|null $key
     * @param string|null $sql
     * @param array $parameters
     * @return string
     */
    protected function resolveDuplicatedValue($table, $key, $sql, array $parameters)
    {
        if (!$table || !$key) {
            return '';
        }

        $indexes = $this->getUniqueIndexes($table);
        $columns = isset($indexes[$key]) ? $indexes[$key] : array();
        if (count($columns) !== 1) {
            return '';
        }

        $attemptedValues = $this->extractAttemptedValues($sql, $parameters);
        $column = $columns[0];

        return array_key_exists($column, $attemptedValues)
            ? (string) $attemptedValues[$column]
            : '';
    }

    /**
     * @param string $table
     * @return array
     */
    protected function getUniqueIndexes($table)
    {
        $indexes = array();
        $escapedTable = SQLite3::escapeString($table);
        $result = $this->connection->query("PRAGMA index_list(\"$escapedTable\")");
        if (!$result) {
            return $indexes;
        }

        while ($index = $result->fetchArray(SQLITE3_ASSOC)) {
            if (empty($index['unique'])) {
                continue;
            }

            $indexName = SQLite3::escapeString(isset($index['name']) ? $index['name'] : '');
            $columns = array();
            $details = $this->connection->query("PRAGMA index_info(\"$indexName\")");
            if ($details) {
                while ($detail = $details->fetchArray(SQLITE3_ASSOC)) {
                    $columns[] = $this->normaliseIdentifier(isset($detail['name']) ? $detail['name'] : '');
                }
                $details->finalize();
            }

            if ($columns) {
                $indexes[$this->normaliseIndexName($table, isset($index['name']) ? $index['name'] : '')] = $columns;
            }
        }

        $result->finalize();

        return $indexes;
    }

    /**
     * @param string|null $sql
     * @param array $parameters
     * @return array
     */
    protected function extractAttemptedValues($sql, array $parameters)
    {
        if (!$sql) {
            return array();
        }

        $parameterValues = array_values($parameters);
        $insertPattern = '/^\s*INSERT\s+INTO\s+.+?\((?<columns>[^)]+)\)\s*(?:VALUES\s*\(|SELECT\s+)/is';
        if (preg_match($insertPattern, $sql, $matches)) {
            $columns = array_map(array($this, 'normaliseIdentifier'), explode(',', $matches['columns']));

            $values = array();
            foreach ($columns as $index => $column) {
                if (array_key_exists($index, $parameterValues)) {
                    $values[$column] = $parameterValues[$index];
                }
            }

            return $values;
        }

        if (preg_match('/^\s*UPDATE\s+.+?\s+SET\s+(?<assignments>.+?)(?:\s+WHERE\s+.+)?\s*$/is', $sql, $matches)) {
            $values = array();
            $parameterIndex = 0;
            $assignments = preg_split('/\s*,\s*/', trim($matches['assignments'])) ?: array();

            foreach ($assignments as $assignment) {
                if (preg_match('/^(?<column>[^=]+)=\s*\?/i', trim($assignment), $assignmentMatch)) {
                    $column = $this->normaliseIdentifier($assignmentMatch['column']);
                    if (array_key_exists($parameterIndex, $parameterValues)) {
                        $values[$column] = $parameterValues[$parameterIndex];
                    }
                    $parameterIndex++;
                    continue;
                }

                $parameterIndex += substr_count($assignment, '?');
            }

            return $values;
        }

        return array();
    }

    /**
     * @param string $table
     * @param array $columns
     * @param array $attemptedValues
     * @return bool
     */
    protected function isUniqueIndexViolated($table, array $columns, array $attemptedValues)
    {
        foreach ($columns as $column) {
            if (!array_key_exists($column, $attemptedValues)) {
                return false;
            }
        }

        $clauses = array();
        foreach ($columns as $column) {
            $escapedColumn = SQLite3::escapeString($column);
            $clauses[] = sprintf('"%s" = ?', $escapedColumn);
        }

        $escapedTable = SQLite3::escapeString($table);
        $statement = $this->connection->prepare(
            sprintf('SELECT 1 FROM "%s" WHERE %s LIMIT 1', $escapedTable, implode(' AND ', $clauses))
        );
        if (!$statement) {
            return false;
        }

        $parsedParameters = call_user_func(
            $this->parsePreparedParameters,
            array_map(function ($column) use ($attemptedValues) {
                return $attemptedValues[$column];
            }, $columns)
        );

        foreach ($parsedParameters as $index => $parameter) {
            $statement->bindValue($index + 1, $parameter['value'], $parameter['type']);
        }

        $result = $statement->execute();
        if (!$result) {
            return false;
        }

        $row = $result->fetchArray(SQLITE3_ASSOC);
        $result->finalize();

        return $row !== false;
    }

    /**
     * @param string $identifier
     * @return string
     */
    protected function normaliseIdentifier($identifier)
    {
        $identifier = trim($identifier);
        if (strpos($identifier, '.') !== false) {
            $parts = explode('.', $identifier);
            $identifier = array_pop($parts);
        }

        return preg_replace('/^"?(.*?)"?$/', '$1', $identifier);
    }

    /**
     * @param string $table
     * @param string $indexName
     * @return string
     */
    protected function normaliseIndexName($table, $indexName)
    {
        $indexName = $this->normaliseIdentifier($indexName);
        $prefix = $this->normaliseIdentifier($table) . '_';

        if (strpos($indexName, $prefix) === 0) {
            return substr($indexName, strlen($prefix));
        }

        return $indexName;
    }
}
