<?php

namespace SilverStripe\SQLite;

/**
 * Transpiles MySQL-oriented ORM SQL into SQLite-compatible SQL.
 */
class SQLite3SQLTranspiler
{
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

        $sql = $this->removeUnionParentheses($sql);
        $sql = $this->rewriteUpdateJoin($sql);
        $sql = $this->rewriteShowKeys($sql);
        $sql = $this->rewriteOrderByField($sql);

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
        return preg_replace_callback(
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
            $sql
        );
    }
}
