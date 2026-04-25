<?php

namespace SilverStripe\SQLite;

use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Environment;

/**
 * Transpiles MySQL-compatible SQL to SQLite-compatible SQL.
 *
 * This class handles transformations between database-specific syntaxes,
 * allowing the ORM to generate MySQL-style SQL while SQLite executes
 * the transpiled version.
 *
 * Configuration:
 * - enable_mysql_compat: Enable optional MySQL compatibility features like
 *   STRAIGHT_JOIN, ON DUPLICATE KEY UPDATE, NOW(), and UNIX_TIMESTAMP() support.
 *   These are disabled by default as they're only needed for custom MySQL queries,
 *   not SilverStripe framework operations. (default: false)
 */
class SQLite3SQLTranspiler
{
    use Configurable;

    /**
     * Enable optional MySQL compatibility features (STRAIGHT_JOIN, ON DUPLICATE KEY UPDATE,
     * NOW(), UNIX_TIMESTAMP()). Disabled by default.
     *
     * @config
     */
    private static bool $enable_mysql_compat = false;

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

        // Framework-required transpilations (always active)
        $sql = $this->removeUnionParentheses($sql);
        $sql = $this->rewriteUpdateJoin($sql);
        $sql = $this->rewriteShowKeys($sql);

        // Optional MySQL compatibility features (opt-in)
        if ($this->config()->get('enable_mysql_compat')) {
            $sql = $this->rewriteStraightJoin($sql);
            $sql = $this->rewriteOnDuplicateKeyUpdate($sql);
            $sql = $this->rewriteNowFunction($sql);
            $sql = $this->rewriteUnixTimestamp($sql);
            $sql = $this->rewriteOrderByField($sql);
        }

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
     * Temporarily replace string literals with placeholders to protect them from regex transformations.
     *
     * @param string $sql
     * @return array{sql: string, literals: array<string>}
     */
    protected function protectStringLiterals(string $sql): array
    {
        $literals = [];
        $counter = 0;

        // Match single-quoted strings, handling escaped quotes ('') and backslash escapes
        $pattern = "/'(?:[^'\\\\]|\\\\.|'')*'/s";

        $sql = preg_replace_callback(
            $pattern,
            function (array $matches) use (&$literals, &$counter): string {
                $placeholder = "\x00LITERAL_" . $counter++ . "\x00";
                $literals[$placeholder] = $matches[0];
                return $placeholder;
            },
            $sql
        );

        return ['sql' => $sql, 'literals' => $literals];
    }

    /**
     * Restore string literals from placeholders.
     *
     * @param string $sql
     * @param array<string> $literals
     * @return string
     */
    protected function restoreStringLiterals(string $sql, array $literals): string
    {
        foreach ($literals as $placeholder => $literal) {
            $sql = str_replace($placeholder, $literal, $sql);
        }
        return $sql;
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
     *
     * @param string $sql
     * @return string
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
     *
     * @param string $sql
     * @return string
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

        if (!empty($matches['where']) && preg_match('/\b"?Key_name"?\s*=\s*\'PRIMARY\'/i', $matches['where'])) {
            return $rewritten;
        }

        return $rewritten;
    }

    /**
     * Rewrite MySQL STRAIGHT_JOIN to regular JOIN for SQLite compatibility.
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteStraightJoin(string $sql): string
    {
        $protected = $this->protectStringLiterals($sql);
        $transformed = preg_replace('/\bSTRAIGHT_JOIN\b/i', 'JOIN', $protected['sql']);
        return $this->restoreStringLiterals($transformed, $protected['literals']);
    }

    /**
     * Rewrite MySQL INSERT ... ON DUPLICATE KEY UPDATE to SQLite UPSERT syntax.
     *
     * Uses the first column from the INSERT as the conflict target.
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteOnDuplicateKeyUpdate(string $sql): string
    {
        // Protect string literals in the SQL first
        $protected = $this->protectStringLiterals($sql);

        $pattern = '/^\s*INSERT\s+INTO\s+(?<table>\S+)\s*\((?<columns>[^)]+)\)\s*'
            . 'VALUES\s*\((?<values>[^)]+)\)\s*'
            . 'ON\s+DUPLICATE\s+KEY\s+UPDATE\s+(?<assignments>.+)$/is';

        if (!preg_match($pattern, $protected['sql'], $matches)) {
            return $sql;
        }

        $columns = preg_split('/\s*,\s*/', trim($matches['columns']));
        $firstColumn = trim($columns[0]);
        $firstColumn = trim($firstColumn, '"\'`[]');

        $rewritten = sprintf(
            'INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) DO UPDATE SET %s',
            trim($matches['table']),
            trim($matches['columns']),
            trim($matches['values']),
            $firstColumn,
            trim($matches['assignments'])
        );

        return $this->restoreStringLiterals($rewritten, $protected['literals']);
    }

    /**
     * Rewrite MySQL NOW() function to SQLite datetime('now').
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteNowFunction(string $sql): string
    {
        $protected = $this->protectStringLiterals($sql);
        $transformed = preg_replace('/\bNOW\s*\(\s*\)/i', "datetime('now')", $protected['sql']);
        return $this->restoreStringLiterals($transformed, $protected['literals']);
    }

    /**
     * Rewrite MySQL UNIX_TIMESTAMP() function to SQLite strftime('%s', ...).
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteUnixTimestamp(string $sql): string
    {
        $protected = $this->protectStringLiterals($sql);
        $transformed = $protected['sql'];

        // Handle UNIX_TIMESTAMP() with no arguments (current time)
        $transformed = preg_replace('/\bUNIX_TIMESTAMP\s*\(\s*\)/i', "strftime('%s', 'now')", $transformed);

        // Handle UNIX_TIMESTAMP(date) with a date argument
        $transformed = preg_replace_callback(
            '/\bUNIX_TIMESTAMP\s*\(([^)]+)\)/i',
            function (array $matches): string {
                $arg = trim($matches[1]);
                return sprintf("strftime('%%s', %s)", $arg);
            },
            $transformed
        );

        return $this->restoreStringLiterals($transformed, $protected['literals']);
    }

    /**
     * Rewrite MySQL ORDER BY FIELD() to SQLite CASE expression.
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteOrderByField(string $sql): string
    {
        return preg_replace_callback(
            '/ORDER\s+BY\s+FIELD\s*\((?<column>[^,]+),(?<values>[^\)]*)\)/i',
            function (array $matches): string {
                $column = trim($matches['column']);
                $values = preg_split('/\s*,\s*/', trim($matches['values'])) ?: [];

                if (empty($values)) {
                    return $matches[0];
                }

                $clauses = [];
                foreach ($values as $index => $value) {
                    $clauses[] = sprintf('WHEN %s THEN %d', trim($value), $index);
                }

                return sprintf(
                    'ORDER BY CASE %s %s ELSE %d END',
                    $column,
                    implode(' ', $clauses),
                    count($values)
                );
            },
            $sql
        );
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
