<?php

namespace SilverStripe\SQLite;

use SilverStripe\Core\Config\Configurable;

/**
 * Transpiles MySQL-oriented ORM SQL into SQLite-compatible SQL.
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
     * @var bool
     */
    private static $enable_mysql_compat = false;

    /**
     * Track if the last transpilation changed the SQL.
     *
     * @var bool
     */
    protected $didTranspile = false;

    /**
     * @param string $sql
     * @return string
     */
    public function transpile($sql)
    {
        $this->didTranspile = false;
        $originalSql = $sql;

        // Framework-required transpilations (always active)
        $sql = $this->removeUnionParentheses($sql);
        $sql = $this->rewriteUpdateJoin($sql);
        $sql = $this->rewriteShowKeys($sql);
        $sql = $this->rewriteOrderByField($sql);

        // Optional MySQL compatibility features (opt-in)
        if ($this->config()->get('enable_mysql_compat')) {
            $sql = $this->rewriteStraightJoin($sql);
            $sql = $this->rewriteOnDuplicateKeyUpdate($sql);
            $sql = $this->rewriteNowFunction($sql);
            $sql = $this->rewriteUnixTimestamp($sql);
        }

        $this->didTranspile = ($sql !== $originalSql);

        return $sql;
    }

    /**
     * @return bool
     */
    public function didTranspile()
    {
        return $this->didTranspile;
    }

    /**
     * Temporarily replace string literals with placeholders to protect them from regex transformations.
     * Returns an array with the modified SQL and the saved literals.
     *
     * @param string $sql
     * @return array{sql: string, literals: array<string>}
     */
    protected function protectStringLiterals($sql)
    {
        $literals = array();
        $counter = 0;

        // Match single-quoted strings, handling escaped quotes ('') and backslash escapes (\', \")
        $pattern = "/'(?:[^'\\\\]|\\\\.|'')*'/s";

        $sql = preg_replace_callback(
            $pattern,
            function ($matches) use (&$literals, &$counter) {
                $placeholder = "\x00LITERAL_" . $counter++ . "\x00";
                $literals[$placeholder] = $matches[0];
                return $placeholder;
            },
            $sql
        );

        return array('sql' => $sql, 'literals' => $literals);
    }

    /**
     * Restore string literals from placeholders.
     *
     * @param string $sql
     * @param array<string> $literals
     * @return string
     */
    protected function restoreStringLiterals($sql, $literals)
    {
        foreach ($literals as $placeholder => $literal) {
            $sql = str_replace($placeholder, $literal, $sql);
        }
        return $sql;
    }

    /**
     * SQLite rejects parentheses around each UNION branch.
     *
     * @param string $sql
     * @return string
     */
    protected function removeUnionParentheses($sql)
    {
        if (!preg_match('/^\s*\(\s*SELECT\b/is', $sql)) {
            return $sql;
        }

        if (!preg_match('/\)\s*UNION(?:\s+(?:ALL|DISTINCT))?\s*\(\s*SELECT\b/is', $sql)) {
            return $sql;
        }

        $sql = preg_replace('/^\s*\(\s*/', '', $sql, 1);
        $sql = preg_replace('/\)\s*(UNION(?:\s+(?:ALL|DISTINCT))?)\s*\(\s*/is', ' $1 ', $sql);
        $sql = preg_replace('/\bUNION\s+DISTINCT\b/i', 'UNION', $sql);

        if (preg_match('/\)\s*$/', $sql)) {
            $sql = preg_replace('/\)\s*$/', '', $sql, 1);
        }

        return $sql;
    }

    /**
     * Rewrite MySQL UPDATE ... INNER JOIN syntax into SQLite UPDATE ... FROM syntax.
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteUpdateJoin($sql)
    {
        $pattern = '/^\s*UPDATE\s+(?<table>.+?)\s+INNER\s+JOIN\s+(?<joinTable>.+?)\s+ON\s+'
            . '(?<joinCondition>.+?)\s+SET\s+(?<assignments>.+?)(?:\s+WHERE\s+(?<where>.+))?\s*$/is';

        if (!preg_match($pattern, $sql, $matches)) {
            return $sql;
        }

        $table = trim($matches['table']);
        $assignments = preg_replace('/' . preg_quote($table, '/') . '\./', '', trim($matches['assignments']));

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
     * Rewrite MySQL SHOW KEYS syntax into a SQLite PRAGMA-backed SELECT.
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteShowKeys($sql)
    {
        $pattern = '/^\s*SHOW\s+KEYS\s+FROM\s+(?<table>"[^"]+"|`[^`]+`|\[[^\]]+\]|\w+)'
            . '(?:\s+WHERE\s+(?<where>.+?))?\s*$/is';

        if (!preg_match($pattern, $sql, $matches)) {
            return $sql;
        }

        $table = trim($matches['table']);
        $table = trim($table, '"`[]');

        $rewritten = sprintf(
            'SELECT name AS "Column_name", \'PRIMARY\' AS "Key_name", pk AS "Seq_in_index" '
            . 'FROM pragma_table_info(\'%s\') WHERE pk > 0',
            str_replace("'", "''", $table)
        );

        if (!empty($matches['where']) && preg_match('/\b"?Key_name"?\s*=\s*\'PRIMARY\'/i', $matches['where'])) {
            return $rewritten;
        }

        return $rewritten;
    }

    /**
     * Rewrite MySQL ORDER BY FIELD(column, ... ) into a SQLite CASE expression.
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteOrderByField($sql)
    {
        $protected = $this->protectStringLiterals($sql);

        $transformed = preg_replace_callback(
            '/ORDER\s+BY\s+FIELD\s*\((?<column>[^,]+),(?<values>[^\)]*)\)/i',
            function ($matches) {
                $column = trim($matches['column']);
                $values = preg_split('/\s*,\s*/', trim($matches['values'])) ?: array();

                if (!$values) {
                    return $matches[0];
                }

                $clauses = array();
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
            $protected['sql']
        );

        return $this->restoreStringLiterals($transformed, $protected['literals']);
    }

    /**
     * Rewrite MySQL STRAIGHT_JOIN to regular JOIN for SQLite compatibility.
     *
     * @param string $sql
     * @return string
     */
    protected function rewriteStraightJoin($sql)
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
    protected function rewriteOnDuplicateKeyUpdate($sql)
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
    protected function rewriteNowFunction($sql)
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
    protected function rewriteUnixTimestamp($sql)
    {
        $protected = $this->protectStringLiterals($sql);
        $transformed = $protected['sql'];

        // Handle UNIX_TIMESTAMP() with no arguments (current time)
        $transformed = preg_replace('/\bUNIX_TIMESTAMP\s*\(\s*\)/i', "strftime('%s', 'now')", $transformed);

        // Handle UNIX_TIMESTAMP(date) with a date argument
        $transformed = preg_replace_callback(
            '/\bUNIX_TIMESTAMP\s*\(([^)]+)\)/i',
            function ($matches) {
                $arg = trim($matches[1]);
                return sprintf("strftime('%%s', %s)", $arg);
            },
            $transformed
        );

        return $this->restoreStringLiterals($transformed, $protected['literals']);
    }
}
