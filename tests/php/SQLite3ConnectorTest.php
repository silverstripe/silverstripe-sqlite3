<?php

declare(strict_types=1);

namespace SilverStripe\SQLite\Tests;

use PHPUnit\Framework\TestCase;
use SilverStripe\SQLite\SQLite3Connector;

final class SQLite3ConnectorTest extends TestCase
{
    /**
     * @dataProvider provideMathExpressions
     */
    public function testMathFunctions(string $expression, ?float $expected): void
    {
        $connector = new SQLite3Connector();
        $connector->connect(['filepath' => ':memory:', 'database' => 'math']);
        try {
            $actual = $connector->query('SELECT ' . $expression . ' AS value')->value();
            if ($expected === null) {
                self::assertNull($actual);
            } else {
                self::assertEqualsWithDelta($expected, (float) $actual, 0.000000000001);
            }
        } finally {
            $connector->unloadDatabase();
        }
    }

    public static function provideMathExpressions(): iterable
    {
        yield 'acos' => ['ACOS(0)', M_PI / 2];
        yield 'sin' => ['SIN(0)', 0.0];
        yield 'cos' => ['COS(0)', 1.0];
        yield 'radians' => ['RADIANS(180)', M_PI];
        yield 'numeric string' => ["RADIANS('180')", M_PI];
        foreach (['ACOS', 'SIN', 'COS', 'RADIANS'] as $function) {
            yield $function . ' null' => [$function . '(NULL)', null];
            yield $function . ' invalid input' => [$function . "('invalid')", null];
        }
        yield 'acos above domain' => ['ACOS(2)', null];
        yield 'acos below domain' => ['ACOS(-2)', null];
    }

    public function testExistingFunctionIsPreserved(): void
    {
        $connector = new class extends SQLite3Connector {
            public function installCustomFunction(): void
            {
                $this->dbConn->createFunction('sin', static fn ($value): float => 42.0, 1);
                $this->registerMathFunctions();
            }
        };
        $connector->connect(['filepath' => ':memory:', 'database' => 'math']);
        try {
            $connector->installCustomFunction();
            self::assertSame(42.0, $connector->query('SELECT SIN(0)')->value());
        } finally {
            $connector->unloadDatabase();
        }
    }

    public function testEscapeStringAcceptsOrmScalarValues(): void
    {
        $connector = new SQLite3Connector();
        $connector->connect(['filepath' => ':memory:', 'database' => 'math']);
        try {
            self::assertSame('50.6333', $connector->escapeString(50.6333));
            self::assertSame('42', $connector->escapeString(42));
            self::assertSame('', $connector->escapeString(null));
            self::assertSame("O''Brien", $connector->escapeString("O'Brien"));
        } finally {
            $connector->unloadDatabase();
        }
    }

    public function testGeographicQuery(): void
    {
        $connector = new SQLite3Connector();
        $connector->connect(['filepath' => ':memory:', 'database' => 'math']);
        try {
            $distance = $connector->query(
                'SELECT ACOS(SIN(RADIANS(0)) * SIN(RADIANS(0))'
                . ' + COS(RADIANS(0)) * COS(RADIANS(0)) * COS(RADIANS(1) - RADIANS(0))) * 6371'
            )->value();
            self::assertEqualsWithDelta(111.194926645, (float) $distance, 0.000001);
        } finally {
            $connector->unloadDatabase();
        }
    }
}
