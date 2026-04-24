<?php

namespace SilverStripe\SQLite;

use SilverStripe\Core\Environment;

/**
 * Transpiles MySQL-compatible SQL to SQLite-compatible SQL
 *
 * This class handles transformations between database-specific syntaxes,
 * allowing the ORM to generate MySQL-style SQL while SQLite executes
 * the transpiled version.
 */
class SQLite3SQLTranspiler
{
    /**
     * Track if any transpilation occurred
     */
    protected bool $didTranspile = false;

    /**
     * Transpile SQL from MySQL syntax to SQLite syntax
     *
     * @param string $sql Original SQL (MySQL-style)
     * @return string Transpiled SQL (SQLite-compatible)
     */
    public function transpile(string $sql): string
    {
        $this->didTranspile = false;
        $originalSql = $sql;

        // Apply transformations in order
        $sql = $this->removeUnionParentheses($sql);
        $sql = $this->rewriteUpdateJoin($sql);
        $sql = $this->rewriteShowKeys($sql);

        // Check if transpilation occurred
        if ($sql !== $originalSql) {
            $this->didTranspile = true;
            $this->logTranspilation($originalSql, $sql);
        }

        return $sql;
    }

    /**
     * Check if the last transpilation actually changed anything
     *
     * @return bool
     */
    public function didTranspile(): bool
    {
        return $this->didTranspile;
    }

    /**
     * Remove parentheses around UNION subqueries
     *
     * MySQL: (SELECT ...) UNION (SELECT ...)
     * SQLite: SELECT ... UNION SELECT ...
     *
     * @param string $sql
     * @return string
     */
    protected function removeUnionParentheses(string $sql): string
    {
        // Check if this looks like a UNION query with outer parentheses
        if (!preg_match('/^\s*\(\s*SELECT\b/is', $sql)) {
            return $sql;
        }

        if (!preg_match('/\)\s*UNION(?:\s+(?:ALL|DISTINCT))?\s*\(\s*SELECT\b/is', $sql)) {
            return $sql;
        }

        $result = $sql;

        // Remove the opening parenthesis at the start of the compound SELECT.
        $result = preg_replace('/^\s*\(\s*/', '', $result, 1);

        // Collapse the outer parentheses around each UNION branch.
        $result = preg_replace(
            '/\)\s*(UNION(?:\s+(?:ALL|DISTINCT))?)\s*\(\s*/is',
            ' $1 ',
            $result
        );

        // SQLite treats UNION as DISTINCT by default and does not accept UNION DISTINCT syntax.
        $result = preg_replace('/\bUNION\s+DISTINCT\b/i', 'UNION', $result);

        // Remove the final outer parenthesis from the last UNION branch.
        if (preg_match('/\)\s*$/', $result)) {
            $result = preg_replace('/\)\s*$/', '', $result, 1);
        }

        return $result;
    }

    /**
     * Rewrite MySQL UPDATE JOIN syntax into SQLite UPDATE ... FROM syntax.
     *
     * MySQL: UPDATE a INNER JOIN b ON b.id = a.id SET a.col = ? WHERE ...
     * SQLite: UPDATE a SET a.col = ? FROM b WHERE b.id = a.id AND (...)
     */
    protected function rewriteUpdateJoin(string $sql): string
    {
        $pattern = '/^\s*UPDATE\s+(?<table>.+?)\s+INNER\s+JOIN\s+(?<joinTable>.+?)\s+ON\s+'
            . '(?<joinCondition>.+?)\s+SET\s+(?<assignments>.+?)(?:\s+WHERE\s+(?<where>.+))?\s*$/is';
        if (!preg_match($pattern, $sql, $matches)) {
            return $sql;
        }

        $table = trim($matches['table']);
        $assignments = preg_replace(
            '/' . preg_quote($table, '/') . '\./',
            '',
            trim($matches['assignments'])
        );

        $rewritten = sprintf(
            'UPDATE %s SET %s FROM %s WHERE %s',
            $table,
            $assignments,
            trim($matches['joinTable']),
            trim($matches['joinCondition'])
        );

        if (!empty($matches['where'])) {
            $rewritten .= sprintf(' AND (%s)', trim($matches['where']));
        }

        return $rewritten;
    }

    /**
     * Rewrite MySQL SHOW KEYS syntax to a SQLite PRAGMA-backed SELECT.
     *
     * Supports the framework test shape:
     * SHOW KEYS FROM table WHERE "Key_name" = 'PRIMARY'
     */
    protected function rewriteShowKeys(string $sql): string
    {
        $pattern = '/^\s*SHOW\s+KEYS\s+FROM\s+(?<table>"[^"]+"|`[^`]+`|\[[^\]]+\]|\w+)'
            . '(?:\s+WHERE\s+(?<where>.+?))?\s*$/is';
        if (!preg_match($pattern, $sql, $matches)) {
            return $sql;
        }

        $table = trim($matches['table']);
        $table = trim($table, '"`[]');

        $rewritten = sprintf(
            "SELECT name AS \"Column_name\", 'PRIMARY' AS \"Key_name\", pk AS \"Seq_in_index\" "
            . "FROM pragma_table_info('%s') WHERE pk > 0",
            str_replace("'", "''", $table)
        );

        if (!empty($matches['where']) && preg_match('/\b\"?Key_name\"?\s*=\s*\'PRIMARY\'/i', $matches['where'])) {
            return $rewritten;
        }

        return $rewritten;
    }

    /**
     * Log transpilation for debugging
     *
     * @param string $original Original SQL
     * @param string $transpiled Transpiled SQL
     */
    protected function logTranspilation(string $original, string $transpiled): void
    {
        if (!$this->isLoggingEnabled()) {
            return;
        }

        $logPath = $this->getLogPath();
        $timestamp = date('Y-m-d H:i:s.u');

        $entry = "[$timestamp]\n";
        $entry .= "ORIGINAL:\n$original\n\n";
        $entry .= "TRANSPILED:\n$transpiled\n";
        $entry .= str_repeat('=', 80) . "\n\n";

        file_put_contents($logPath, $entry, FILE_APPEND | LOCK_EX);
    }

    /**
     * Check if transpilation logging is enabled
     *
     * @return bool
     */
    protected function isLoggingEnabled(): bool
    {
        $envValue = getenv('SS_SQLITE_LOG_TRANSPILE');
        if ($envValue === false) {
            $envValue = Environment::getEnv('SS_SQLITE_LOG_TRANSPILE');
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
        return $this->findProjectRoot() . '/sqlite3_transpile.log';
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
}
