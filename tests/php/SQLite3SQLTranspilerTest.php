<?php

namespace SilverStripe\SQLite\Tests;

use PHPUnit\Framework\TestCase;
use SilverStripe\SQLite\SQLite3SQLTranspiler;

/**
 * Tests for the SQL Transpiler
 */
class SQLite3SQLTranspilerTest extends TestCase
{
    protected $transpiler;

    protected function setUp(): void
    {
        parent::setUp();
        $this->transpiler = new SQLite3SQLTranspiler();
    }

    /**
     * Test that UNION parentheses are removed correctly
     */
    public function testUnionParenthesesRemoval()
    {
        // MySQL-style UNION with parentheses
        $mysqlSql = '(
 SELECT DISTINCT "Permission"."Code"
 FROM "Permission"
 WHERE ("Permission"."Type" = ?)
 )
 UNION
 (
 SELECT DISTINCT "PermissionRoleCode"."Code"
 FROM "PermissionRoleCode"
 WHERE ("GroupID" IN (?))
 )';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // SQLite should not have outer parentheses around SELECTs
        $this->assertStringNotContainsString('( SELECT', $sqliteSql);
        $this->assertStringNotContainsString(') UNION', $sqliteSql);

        // But should still contain the UNION
        $this->assertStringContainsString('UNION', $sqliteSql);

        // Verify it's valid SQLite-style
        $this->assertStringContainsString('SELECT DISTINCT', $sqliteSql);
    }

    /**
     * Test that UNION ALL is handled correctly
     */
    public function testUnionAllParenthesesRemoval()
    {
        $mysqlSql = '(
 SELECT "ID" FROM "Table1"
 )
 UNION ALL
 (
 SELECT "ID" FROM "Table2"
 )';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringNotContainsString('( SELECT', $sqliteSql);
        $this->assertStringNotContainsString(') UNION ALL', $sqliteSql);
        $this->assertStringContainsString('UNION ALL', $sqliteSql);
    }

    /**
     * Test that UNION DISTINCT is handled correctly
     */
    public function testUnionDistinctParenthesesRemoval()
    {
        $mysqlSql = '(

SELECT 1, 2
 )
 UNION DISTINCT
 (

SELECT 1, 2
 )';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringContainsString('UNION', $sqliteSql);
        $this->assertStringNotContainsString('UNION DISTINCT', $sqliteSql);
        $this->assertStringNotContainsString('(

SELECT', $sqliteSql);
        $this->assertStringNotContainsString(')\n UNION\n (', $sqliteSql);
    }

    /**
     * Test that UPDATE INNER JOIN is rewritten to SQLite UPDATE FROM syntax
     */
    public function testUpdateInnerJoinRewrite()
    {
        $mysqlSql = 'UPDATE "SQLUpdateTestBase"
INNER JOIN "SQLUpdateTestOther" ON "SQLUpdateTestOther"."Description" = "SQLUpdateTestBase"."Description"
 SET "SQLUpdateTestBase"."Description" = ?';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $expectedSql = 'UPDATE "SQLUpdateTestBase" SET "Description" = ? FROM "SQLUpdateTestOther" '
            . 'WHERE "SQLUpdateTestOther"."Description" = "SQLUpdateTestBase"."Description"';

        $this->assertSame(
            $expectedSql,
            $sqliteSql
        );
    }

    public function testShowKeysRewrite()
    {
        $mysqlSql = 'SHOW KEYS FROM _sessions WHERE "Key_name" = \'PRIMARY\'';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertSame(
            'SELECT name AS "Column_name", \'PRIMARY\' AS "Key_name", pk AS "Seq_in_index" '
            . 'FROM pragma_table_info(\'_sessions\') WHERE pk > 0',
            $sqliteSql
        );
        $this->assertTrue($this->transpiler->didTranspile());
    }

    public function testOrderByFieldRewrite()
    {
        $mysqlSql = 'SELECT *
FROM "EagerLoadObject_ManyManyEagerLoadObjects"
WHERE "EagerLoadObjectID" IN (1,2) AND ManyManyEagerLoadObjectID IN (1,2,5,6)
ORDER BY FIELD(ManyManyEagerLoadObjectID, 1,2,5,6)';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertSame(
            'SELECT *
FROM "EagerLoadObject_ManyManyEagerLoadObjects"
WHERE "EagerLoadObjectID" IN (1,2) AND ManyManyEagerLoadObjectID IN (1,2,5,6)
ORDER BY CASE ManyManyEagerLoadObjectID WHEN 1 THEN 0 WHEN 2 THEN 1 WHEN 5 THEN 2 WHEN 6 THEN 3 ELSE 4 END',
            $sqliteSql
        );
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that non-UNION queries are not modified
     */
    public function testNonUnionQueriesUnchanged()
    {
        $sql = 'SELECT * FROM "SiteTree" WHERE "ID" = ?';
        $result = $this->transpiler->transpile($sql);

        $this->assertEquals($sql, $result);
        $this->assertFalse($this->transpiler->didTranspile());
    }

    /**
     * Test that queries with subquery parentheses are preserved
     */
    public function testSubqueryParenthesesPreserved()
    {
        $sql = 'SELECT * FROM "SiteTree" WHERE "ID" IN (SELECT "ID" FROM "OtherTable")';
        $result = $this->transpiler->transpile($sql);

        // Inner parentheses should remain
        $this->assertStringContainsString('(SELECT "ID"', $result);
    }
}
