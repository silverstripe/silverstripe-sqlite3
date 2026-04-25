<?php

namespace SilverStripe\SQLite\Tests;

use SilverStripe\Core\Config\Config;
use SilverStripe\Dev\SapphireTest;
use SilverStripe\SQLite\SQLite3SQLTranspiler;

/**
 * Tests for the SQL Transpiler
 */
class SQLite3SQLTranspilerTest extends SapphireTest
{
    protected SQLite3SQLTranspiler $transpiler;

    protected function setUp(): void
    {
        parent::setUp();
        $this->transpiler = new SQLite3SQLTranspiler();
        // Ensure MySQL compat is disabled by default for tests
        Config::modify()->set(SQLite3SQLTranspiler::class, 'enable_mysql_compat', false);
    }

    protected function tearDown(): void
    {
        // Reset config after each test
        Config::modify()->set(SQLite3SQLTranspiler::class, 'enable_mysql_compat', false);
        parent::tearDown();
    }

    /**
     * Helper to enable MySQL compatibility mode
     */
    protected function enableMySQLCompat(): void
    {
        Config::modify()->set(SQLite3SQLTranspiler::class, 'enable_mysql_compat', true);
    }

    /**
     * Test that UNION parentheses are removed correctly
     */
    public function testUnionParenthesesRemoval(): void
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
    public function testUnionAllParenthesesRemoval(): void
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
    public function testUnionDistinctParenthesesRemoval(): void
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
    public function testUpdateInnerJoinRewrite(): void
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

    /**
     * Test SHOW KEYS rewrite
     */
    public function testShowKeysRewrite(): void
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

    /**
     * Test that non-UNION queries are not modified
     */
    public function testNonUnionQueriesUnchanged(): void
    {
        $sql = 'SELECT * FROM "SiteTree" WHERE "ID" = ?';
        $result = $this->transpiler->transpile($sql);

        $this->assertEquals($sql, $result);
        $this->assertFalse($this->transpiler->didTranspile());
    }

    /**
     * Test that queries with subquery parentheses are preserved
     */
    public function testSubqueryParenthesesPreserved(): void
    {
        $sql = 'SELECT * FROM "SiteTree" WHERE "ID" IN (SELECT "ID" FROM "OtherTable")';
        $result = $this->transpiler->transpile($sql);

        // Inner parentheses should remain
        $this->assertStringContainsString('(SELECT "ID"', $result);
    }

    /**
     * Test that STRAIGHT_JOIN is NOT converted when MySQL compat is disabled
     */
    public function testStraightJoinNotConvertedWhenDisabled(): void
    {
        $mysqlSql = 'SELECT * FROM "Table1" STRAIGHT_JOIN "Table2" ON "Table1"."ID" = "Table2"."ID"';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // Should remain unchanged when MySQL compat is disabled
        $this->assertStringContainsString('STRAIGHT_JOIN', $sqliteSql);
        $this->assertFalse($this->transpiler->didTranspile());
    }

    /**
     * Test that STRAIGHT_JOIN is converted to regular JOIN when MySQL compat is enabled
     */
    public function testStraightJoinRewrite(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT * FROM "Table1" STRAIGHT_JOIN "Table2" ON "Table1"."ID" = "Table2"."ID"';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringNotContainsString('STRAIGHT_JOIN', $sqliteSql);
        $this->assertStringContainsString('JOIN', $sqliteSql);
        $this->assertStringContainsString('FROM "Table1" JOIN "Table2"', $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test case-insensitive STRAIGHT_JOIN handling when MySQL compat is enabled
     */
    public function testStraightJoinCaseInsensitive(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT * FROM t1 straight_join t2 ON t1.id = t2.id';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringNotContainsString('straight_join', $sqliteSql);
        $this->assertStringNotContainsString('STRAIGHT_JOIN', $sqliteSql);
        $this->assertStringContainsString('JOIN', $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that ON DUPLICATE KEY UPDATE is NOT rewritten when MySQL compat is disabled
     */
    public function testOnDuplicateKeyUpdateNotRewrittenWhenDisabled(): void
    {
        $mysqlSql = 'INSERT INTO "MyTable" ("ID", "Name", "Value") VALUES (1, \'Test\', 100) '
            . 'ON DUPLICATE KEY UPDATE "Name" = \'Updated\', "Value" = 200';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // Should remain unchanged when MySQL compat is disabled
        $this->assertStringContainsString('ON DUPLICATE KEY UPDATE', $sqliteSql);
        $this->assertFalse($this->transpiler->didTranspile());
    }

    /**
     * Test that ON DUPLICATE KEY UPDATE is rewritten to SQLite UPSERT when MySQL compat is enabled
     */
    public function testOnDuplicateKeyUpdateRewrite(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'INSERT INTO "MyTable" ("ID", "Name", "Value") VALUES (1, \'Test\', 100) '
            . 'ON DUPLICATE KEY UPDATE "Name" = \'Updated\', "Value" = 200';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $expectedSql = 'INSERT INTO "MyTable" ("ID", "Name", "Value") VALUES (1, \'Test\', 100) '
            . 'ON CONFLICT(ID) DO UPDATE SET "Name" = \'Updated\', "Value" = 200';

        $this->assertSame($expectedSql, $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test case-insensitive ON DUPLICATE KEY UPDATE handling when MySQL compat is enabled
     */
    public function testOnDuplicateKeyUpdateCaseInsensitive(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'insert into users (id, name) values (1, \'john\') on duplicate key update name = \'jane\'';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringContainsString('ON CONFLICT(id) DO UPDATE SET', $sqliteSql);
        $this->assertStringNotContainsString('ON DUPLICATE KEY UPDATE', $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that NOW() is NOT rewritten when MySQL compat is disabled
     */
    public function testNowFunctionNotRewrittenWhenDisabled(): void
    {
        $mysqlSql = 'INSERT INTO "Log" ("Message", "Created") VALUES (\'Test\', NOW())';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // Should remain unchanged when MySQL compat is disabled
        $this->assertStringContainsString('NOW()', $sqliteSql);
        $this->assertFalse($this->transpiler->didTranspile());
    }

    /**
     * Test that NOW() is rewritten to datetime('now') when MySQL compat is enabled
     */
    public function testNowFunctionRewrite(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'INSERT INTO "Log" ("Message", "Created") VALUES (\'Test\', NOW())';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $expectedSql = "INSERT INTO \"Log\" (\"Message\", \"Created\") VALUES ('Test', datetime('now'))";

        $this->assertSame($expectedSql, $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test case-insensitive NOW() handling when MySQL compat is enabled
     */
    public function testNowFunctionCaseInsensitive(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT * FROM "Events" WHERE "StartTime" < now()';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringContainsString("datetime('now')", $sqliteSql);
        $this->assertStringNotContainsString('now()', $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that UNIX_TIMESTAMP() is NOT rewritten when MySQL compat is disabled
     */
    public function testUnixTimestampNotRewrittenWhenDisabled(): void
    {
        $mysqlSql = 'SELECT UNIX_TIMESTAMP() AS "CurrentTime"';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // Should remain unchanged when MySQL compat is disabled
        $this->assertStringContainsString('UNIX_TIMESTAMP()', $sqliteSql);
        $this->assertFalse($this->transpiler->didTranspile());
    }

    /**
     * Test that UNIX_TIMESTAMP() without argument is rewritten when MySQL compat is enabled
     */
    public function testUnixTimestampNoArgsRewrite(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT UNIX_TIMESTAMP() AS "CurrentTime"';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $expectedSql = "SELECT strftime('%s', 'now') AS \"CurrentTime\"";

        $this->assertSame($expectedSql, $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that UNIX_TIMESTAMP(date) with argument is rewritten when MySQL compat is enabled
     */
    public function testUnixTimestampWithArgRewrite(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT UNIX_TIMESTAMP("Created") AS "UnixTime" FROM "Events"';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $expectedSql = "SELECT strftime('%s', \"Created\") AS \"UnixTime\" FROM \"Events\"";

        $this->assertSame($expectedSql, $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test case-insensitive UNIX_TIMESTAMP handling when MySQL compat is enabled
     */
    public function testUnixTimestampCaseInsensitive(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'select unix_timestamp() from test';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringContainsString("strftime('%s', 'now')", $sqliteSql);
        $this->assertStringNotContainsString('unix_timestamp', $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that ORDER BY FIELD is NOT rewritten when MySQL compat is disabled
     */
    public function testOrderByFieldNotRewrittenWhenDisabled(): void
    {
        $mysqlSql = 'SELECT * FROM "Table" ORDER BY FIELD("Status", \'active\', \'pending\', \'archived\')';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // Should remain unchanged when MySQL compat is disabled
        $this->assertStringContainsString('ORDER BY FIELD', $sqliteSql);
        $this->assertFalse($this->transpiler->didTranspile());
    }

    /**
     * Test that ORDER BY FIELD is rewritten to CASE expression when MySQL compat is enabled
     */
    public function testOrderByFieldRewrite(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT * FROM "Table" ORDER BY FIELD("Status", \'active\', \'pending\', \'archived\')';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $expectedSql = 'SELECT * FROM "Table" ORDER BY CASE "Status" '
            . 'WHEN \'active\' THEN 0 WHEN \'pending\' THEN 1 WHEN \'archived\' THEN 2 ELSE 3 END';

        $this->assertSame($expectedSql, $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that ORDER BY FIELD with numbers is rewritten correctly when MySQL compat is enabled
     */
    public function testOrderByFieldWithNumbers(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT * FROM "Items" ORDER BY FIELD(ID, 5, 2, 8, 1)';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        $this->assertStringContainsString(
            'ORDER BY CASE ID WHEN 5 THEN 0 WHEN 2 THEN 1 WHEN 8 THEN 2 WHEN 1 THEN 3 ELSE 4 END',
            $sqliteSql
        );
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that NOW() inside string literals is not transformed when MySQL compat is enabled
     */
    public function testNowFunctionInStringLiteralNotTransformed(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'INSERT INTO "Log" ("Message", "Help") VALUES (\'Use NOW() to get current time\', NOW())';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // The NOW() inside the string literal should remain unchanged
        $this->assertStringContainsString("'Use NOW() to get current time'", $sqliteSql);
        // The NOW() outside the string literal should be transformed
        $this->assertStringContainsString("datetime('now')", $sqliteSql);
        // Make sure we don't have the literal transformed
        $this->assertStringNotContainsString("'Use datetime('now') to get current time'", $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that UNIX_TIMESTAMP() inside string literals is not transformed when MySQL compat is enabled
     */
    public function testUnixTimestampInStringLiteralNotTransformed(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'SELECT UNIX_TIMESTAMP(), \'UNIX_TIMESTAMP() returns seconds\' FROM "Test"';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // The UNIX_TIMESTAMP() inside the string literal should remain unchanged
        $this->assertStringContainsString("'UNIX_TIMESTAMP() returns seconds'", $sqliteSql);
        // The UNIX_TIMESTAMP() outside the string literal should be transformed
        $this->assertStringContainsString("strftime('%s', 'now')", $sqliteSql);
        // Make sure we don't have the literal transformed
        $this->assertStringNotContainsString("'strftime('%s', 'now') returns seconds'", $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that STRAIGHT_JOIN inside string literals is not transformed when MySQL compat is enabled
     */
    public function testStraightJoinInStringLiteralNotTransformed(): void
    {
        $this->enableMySQLCompat();

        // This query has STRAIGHT_JOIN as SQL keyword (not inside a string literal)
        $mysqlSql = 'SELECT \'Use STRAIGHT_JOIN syntax\' AS hint FROM t1 STRAIGHT_JOIN t2 ON t1.id = t2.id';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // The STRAIGHT_JOIN inside the string literal should remain unchanged
        $this->assertStringContainsString("'Use STRAIGHT_JOIN syntax'", $sqliteSql);
        $this->assertStringNotContainsString("'Use JOIN syntax'", $sqliteSql);
        // The STRAIGHT_JOIN outside should be transformed to JOIN
        $this->assertStringContainsString('FROM t1 JOIN t2', $sqliteSql);
        $this->assertTrue($this->transpiler->didTranspile());
    }

    /**
     * Test that escaped quotes inside string literals are handled correctly
     */
    public function testEscapedQuotesInStringLiterals(): void
    {
        $this->enableMySQLCompat();

        $mysqlSql = 'INSERT INTO "Test" ("Message") VALUES (\'It\'\'s NOW() time\')';

        $sqliteSql = $this->transpiler->transpile($mysqlSql);

        // The escaped quote and NOW() inside should remain unchanged
        $this->assertStringContainsString("'It''s NOW() time'", $sqliteSql);
        $this->assertFalse($this->transpiler->didTranspile());
    }
}
