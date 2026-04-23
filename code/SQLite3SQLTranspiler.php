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
     *
     * @var bool
     */
    protected $didTranspile = false;

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
        if (!preg_match('/^\s*\(\s*SELECT/i', $sql) || !preg_match('/\)\s*UNION\s*\(/i', $sql)) {
            return $sql;
        }

        $result = $sql;

        // Step 1: Remove the opening parenthesis at the very start (before first SELECT)
        $result = preg_replace('/^\s*\(\s*(SELECT\s)/i', '$1', $result);

        // Step 2: Remove the pattern: ) UNION ( or ) UNION ALL ( or ) UNION DISTINCT (
        // Replace with: ) UNION or ) UNION ALL or ) UNION DISTINCT
        $result = preg_replace(
            '/\)\s*(UNION(?:\s+(?:ALL|DISTINCT))?)\s*\(\s*(SELECT\s)/i',
            ') $1 $2',
            $result
        );

        // Step 3: Remove the closing parenthesis before UNION
        // Pattern: closing ) followed by UNION - remove the )
        $result = preg_replace('/\)\s+(UNION\s)/i', ' $1', $result);

        // Step 4: Remove the closing parenthesis at the end if there's a UNION before it
        // This removes the trailing ) after the last SELECT in a UNION
        // We need to be careful because there may be parentheses inside WHERE clauses
        if (preg_match('/UNION/i', $result)) {
            // Find trailing ) that appears after the last character that's not )
            // by trimming trailing whitespace then checking if last char is )
            $trimmed = rtrim($result);
            if (substr($trimmed, -1) === ')') {
                // Remove the final )
                $result = substr($trimmed, 0, -1);
            }
        }

        return $result;
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
