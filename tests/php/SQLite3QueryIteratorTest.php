<?php

namespace SilverStripe\SQLite\Tests;

use SilverStripe\Dev\SapphireTest;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\DB;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\View\TemplateEngine;
use SilverStripe\View\ViewLayerData;

/**
 * Tests for SQLite3 Query Iterator behavior
 *
 * These tests verify proper DataList iteration behavior with SQLite3.
 * They serve as regression tests for potential iterator issues.
 */
class SQLite3QueryIteratorTest extends SapphireTest
{
    protected static $fixture_file = null;

    protected static $extra_dataobjects = [
        TestDataObject::class,
    ];

    protected static $required_extensions = [
        TestDataObject::class => [],
    ];

    protected function setUp(): void
    {
        parent::setUp();

        // These tests require SQLite - skip if not using SQLite
        $dbClass = DB::get_conn();
        if (!$dbClass || !($dbClass instanceof \SilverStripe\SQLite\SQLite3Database)) {
            $this->markTestSkipped('This test requires SQLite3 database (use DB=SQLITE)');
        }
    }

    /**
     * Test that iterating over a result doesn't duplicate items on rewind
     *
     * This replicates the bug from GitHub Issue #73 where a single record
     * in a DataList would be displayed twice in templates due to iterator issues.
     */
    public function testIteratorDoesNotDuplicateItemsOnRewind()
    {
        // Create a simple test DataObject
        $obj = new TestDataObject();
        $obj->Title = 'Test Record';
        $obj->write();

        // Get the record as a DataList (simulating what happens in templates)
        $list = TestDataObject::get()->filter('ID', $obj->ID);

        // Count should be 1
        $this->assertEquals(1, $list->count(), 'List should contain exactly one record');

        // First iteration - collect items
        $firstPass = [];
        foreach ($list as $item) {
            $firstPass[] = $item->ID;
        }

        // Second iteration (simulating what SSViewer_Scope does with rewind)
        // This is where the bug would cause duplication
        $secondPass = [];
        foreach ($list as $item) {
            $secondPass[] = $item->ID;
        }

        // Both passes should have the same single item
        $this->assertCount(1, $firstPass, 'First iteration should have exactly one item');
        $this->assertCount(1, $secondPass, 'Second iteration should have exactly one item (bug would show 2)');
        $this->assertEquals($firstPass, $secondPass, 'Both iterations should return same items');
        $this->assertEquals($obj->ID, $firstPass[0], 'First pass should contain the test record ID');
        $this->assertEquals($obj->ID, $secondPass[0], 'Second pass should contain the test record ID');

        // Clean up
        $obj->delete();
    }

    /**
     * Test that numRecords doesn't interfere with iteration
     *
     * Calling count() before iteration used to cause the iterator to start
     * from the wrong position due to the reset() call in numRecords().
     */
    public function testCountBeforeIterationDoesNotCauseDuplication()
    {
        // Create multiple test records
        $ids = [];
        for ($i = 1; $i <= 3; $i++) {
            $obj = new TestDataObject();
            $obj->Title = "Record $i";
            $obj->write();
            $ids[] = $obj->ID;
        }

        $list = TestDataObject::get()->sort('ID');

        // Call count() first (this used to trigger the bug)
        $count = $list->count();
        $this->assertEquals(3, $count, 'Count should return 3');

        // Now iterate - before the fix, this would show items twice
        $iteratedIds = [];
        foreach ($list as $item) {
            $iteratedIds[] = $item->ID;
        }

        // Should have exactly 3 items, not 6
        $this->assertCount(3, $iteratedIds, 'Should iterate exactly 3 items, not duplicated');
        $this->assertEquals($ids, $iteratedIds, 'Iterated IDs should match created IDs');

        // Clean up
        foreach ($list as $obj) {
            $obj->delete();
        }
    }

    /**
     * Test that SQLite3Query::numRecords() doesn't duplicate rows on later iteration
     */
    public function testDirectQueryCountBeforeIterationDoesNotDuplicateRows()
    {
        $ids = [];
        for ($i = 1; $i <= 3; $i++) {
            $obj = new TestDataObject();
            $obj->Title = "Direct Record $i";
            $obj->write();
            $ids[] = $obj->ID;
        }

        $query = DB::query(
            sprintf(
                'SELECT "ID" FROM "%s" WHERE "ID" IN (%s) ORDER BY "ID"',
                TestDataObject::config()->get('table_name'),
                implode(', ', $ids)
            )
        );

        $this->assertEquals(3, $query->numRecords(), 'Direct query count should return 3');

        $iteratedIds = [];
        foreach ($query as $row) {
            $iteratedIds[] = (int) $row['ID'];
        }

        $this->assertSame($ids, $iteratedIds, 'Direct query iteration should return each row once');

        TestDataObject::get()->filter('ID', $ids)->removeAll();
    }

    /**
     * Test iterator with single item (the exact scenario from Issue #73)
     */
    public function testSingleItemNotDuplicated()
    {
        $obj = new TestDataObject();
        $obj->Title = 'Single Item';
        $obj->write();

        $list = TestDataObject::get()->filter('ID', $obj->ID);

        // Simulate template behavior: check count, then iterate
        $count = $list->count();
        $this->assertEquals(1, $count);

        // Get iterator and convert to array
        $items = [];
        foreach ($list as $item) {
            $items[] = $item;
        }

        // Should have exactly 1 item (no duplicates)
        $this->assertCount(1, $items, 'Single item list should not duplicate');

        $obj->delete();
    }

    /**
     * Test DataList iteration through template rendering (SS6 API)
     *
     * This test uses the SilverStripe 6 TemplateEngine API to render a template
     * with a DataList loop. It verifies that items appear exactly once in the
     * rendered output, not duplicated.
     *
     * In SS6, TemplateEngine::renderString() replaces SSViewer::fromString()
     */
    public function testTemplateLoopDoesNotDuplicateItems()
    {
        // Create test records
        $expectedTitles = [];
        for ($i = 1; $i <= 3; $i++) {
            $obj = new TestDataObject();
            $obj->Title = "Record $i";
            $obj->write();
            $expectedTitles[] = "Record $i";
        }

        // Get DataList
        $list = TestDataObject::get()->sort('ID');

        // Create template with loop using SS6 TemplateEngine API
        $template = '<% loop List %>[{$Title}]<% end_loop %>';

        // Render using TemplateEngine (SS6 way)
        $engine = Injector::inst()->create(TemplateEngine::class);
        $model = new ViewLayerData(['List' => $list]);
        $result = $engine->renderString($template, $model);

        // Parse the result to extract titles
        preg_match_all('/\[([^\]]+)\]/', $result, $matches);
        $renderedTitles = $matches[1] ?? [];

        // Verify we got exactly 3 items, not 6 (duplicated)
        $this->assertCount(
            3,
            $renderedTitles,
            'Template should render exactly 3 items, not duplicated. Got: ' . json_encode($renderedTitles)
        );
        $this->assertEquals($expectedTitles, $renderedTitles, 'Rendered titles should match expected');

        // Clean up
        foreach ($list as $obj) {
            $obj->delete();
        }
    }
}
