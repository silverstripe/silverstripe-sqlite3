<?php

namespace SilverStripe\SQLite;

/**
 * Transpiles MySQL-oriented ORM SQL into SQLite-compatible SQL.
 */
class SQLite3SQLTranspiler
{
    /**
     * @param string $sql
     * @return string
     */
    public function transpile($sql)
    {
        $sql = $this->removeUnionParentheses($sql);
        $sql = $this->rewriteUpdateJoin($sql);

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
}
