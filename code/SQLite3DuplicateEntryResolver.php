<?php

namespace SilverStripe\SQLite;

use SQLite3;

class SQLite3DuplicateEntryResolver
{
    private SQLite3 $connection;

    /**
     * @var callable
     */
    private $parsePreparedParameters;

    public function __construct(SQLite3 $connection, callable $parsePreparedParameters)
    {
        $this->connection = $connection;
        $this->parsePreparedParameters = $parsePreparedParameters;
    }

    /**
     * @return array{key: string|null, value: string}
     */
    public function resolve(string $constraintFields, ?string $sql, array $parameters): array
    {
        [$table, $fields] = $this->parseUniqueConstraintFields($constraintFields);
        $key = $this->resolveDuplicateKeyName($table, $fields, $sql, $parameters);
        $value = $this->resolveDuplicatedValue($table, $key, $sql, $parameters);

        return [
            'key' => $key,
            'value' => $value,
        ];
    }

    /**
     * @return array{0: string|null, 1: array}
     */
    private function parseUniqueConstraintFields(string $constraintFields): array
    {
        $fields = [];
        $table = null;

        foreach (explode(',', $constraintFields) as $field) {
            $field = trim($field);
            if ($field === '') {
                continue;
            }

            if (str_contains($field, '.')) {
                $parts = explode('.', $field);
                $field = array_pop($parts);
                $table = $parts[0] ?? $table;
            }

            $fields[] = $this->normaliseIdentifier($field);
        }

        return [$table, $fields];
    }

    private function resolveDuplicateKeyName(?string $table, array $fields, ?string $sql, array $parameters): ?string
    {
        if (!$table) {
            return implode(', ', $fields);
        }

        $indexes = $this->getUniqueIndexes($table);
        $attemptedValues = $this->extractAttemptedValues($sql, $parameters);
        $violatedIndexes = [];

        foreach ($indexes as $indexName => $columns) {
            if ($this->isUniqueIndexViolated($table, $columns, $attemptedValues)) {
                $violatedIndexes[$indexName] = $columns;
            }
        }

        if ($violatedIndexes) {
            uasort(
                $violatedIndexes,
                static fn(array $left, array $right): int => count($left) <=> count($right)
            );
            return array_key_first($violatedIndexes);
        }

        foreach ($indexes as $indexName => $columns) {
            if ($columns === $fields) {
                return $indexName;
            }
        }

        return implode(', ', $fields);
    }

    private function resolveDuplicatedValue(?string $table, ?string $key, ?string $sql, array $parameters): string
    {
        if (!$table || !$key) {
            return '';
        }

        $indexes = $this->getUniqueIndexes($table);
        $columns = $indexes[$key] ?? [];
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
     * @return array<string, array<int, string>>
     */
    private function getUniqueIndexes(string $table): array
    {
        $indexes = [];
        $escapedTable = SQLite3::escapeString($table);
        $result = $this->connection->query("PRAGMA index_list(\"$escapedTable\")");
        if (!$result) {
            return $indexes;
        }

        while ($index = $result->fetchArray(SQLITE3_ASSOC)) {
            if (empty($index['unique'])) {
                continue;
            }

            $indexName = SQLite3::escapeString($index['name'] ?? '');
            $columns = [];
            $details = $this->connection->query("PRAGMA index_info(\"$indexName\")");
            if ($details) {
                while ($detail = $details->fetchArray(SQLITE3_ASSOC)) {
                    $columns[] = $this->normaliseIdentifier($detail['name'] ?? '');
                }
                $details->finalize();
            }

            if ($columns) {
                $indexes[$this->normaliseIndexName($table, $index['name'] ?? '')] = $columns;
            }
        }

        $result->finalize();

        return $indexes;
    }

    /**
     * @return array<string, mixed>
     */
    private function extractAttemptedValues(?string $sql, array $parameters): array
    {
        if (!$sql) {
            return [];
        }

        $parameterValues = array_values($parameters);
        $insertPattern = '/^\s*INSERT\s+INTO\s+.+?\((?<columns>[^)]+)\)\s*'
            . '(?:VALUES\s*\(|SELECT\s+)/is';
        if (preg_match($insertPattern, $sql, $matches)) {
            $columns = array_map(
                fn(string $column): string => $this->normaliseIdentifier($column),
                explode(',', $matches['columns'])
            );

            $values = [];
            foreach ($columns as $index => $column) {
                if (array_key_exists($index, $parameterValues)) {
                    $values[$column] = $parameterValues[$index];
                }
            }

            return $values;
        }

        if (preg_match('/^\s*UPDATE\s+.+?\s+SET\s+(?<assignments>.+?)(?:\s+WHERE\s+.+)?\s*$/is', $sql, $matches)) {
            $values = [];
            $parameterIndex = 0;
            $assignments = preg_split('/\s*,\s*/', trim($matches['assignments'])) ?: [];

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

        return [];
    }

    private function isUniqueIndexViolated(string $table, array $columns, array $attemptedValues): bool
    {
        foreach ($columns as $column) {
            if (!array_key_exists($column, $attemptedValues)) {
                return false;
            }
        }

        $clauses = [];
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

        $parsedParameters = ($this->parsePreparedParameters)(array_map(
            fn(string $column) => $attemptedValues[$column],
            $columns
        ));

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

    private function normaliseIdentifier(string $identifier): string
    {
        $identifier = trim($identifier);
        if (str_contains($identifier, '.')) {
            $parts = explode('.', $identifier);
            $identifier = array_pop($parts);
        }

        return preg_replace('/^"?(.*?)"?$/', '$1', $identifier ?? '') ?? $identifier;
    }

    private function normaliseIndexName(string $table, string $indexName): string
    {
        $indexName = $this->normaliseIdentifier($indexName);
        $prefix = $this->normaliseIdentifier($table) . '_';

        if (str_starts_with($indexName, $prefix)) {
            return substr($indexName, strlen($prefix));
        }

        return $indexName;
    }
}
