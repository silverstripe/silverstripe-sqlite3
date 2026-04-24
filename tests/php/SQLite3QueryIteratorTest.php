<?php

namespace SilverStripe\SQLite\Tests;

use SilverStripe\Dev\SapphireTest;
use SilverStripe\ORM\DB;
use SilverStripe\View\ArrayData;
use SilverStripe\View\SSViewer;

/**
 * Tests for SQLite3 Query Iterator behavior
 *
 * These tests verify proper DataList iteration behavior with SQLite3,
 * particularly that iterators can be rewound and reused after count() operations.
 */
class SQLite3QueryIteratorTest extends SapphireTest
{
    protected static $fixture_file = null;

    protected static $extra_dataobjects = [
        TestDataObject::class,
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
     * Test that iterator works correctly after count() is called
     *
     * This was the root cause of GitHub Issue #73 where items would be
     * duplicated or missing after count() operations.
     */
    public function testIteratorWorksAfterCount()
    {
        // Create test records
        $expectedIds = [];
        for ($i = 1; $i <= 3; $i++) {
            $obj = new TestDataObject();
            $obj->Title = "Record $i";
            $obj->write();
            $expectedIds[] = $obj->ID;
        }

        $list = TestDataObject::get()->sort('ID');

        // Call count() first - this used to exhaust the iterator
        $count = $list->count();
        $this->assertEquals(3, $count, 'Count should return 3');

        // Now iterate - this should work correctly
        $iteratedIds = [];
        foreach ($list as $item) {
            $iteratedIds[] = $item->ID;
        }

        // Should have exactly 3 items, not duplicated or missing
        $this->assertEquals($expectedIds, $iteratedIds, 'Iterator should return all items after count()');

        // Clean up
        foreach ($list as $obj) {
            $obj->delete();
        }
    }

    /**
     * Test that DataList iteration works correctly in SSViewer templates
     *
     * This verifies the fix works for the actual use case - template rendering.
     * SSViewer calls count() then iterates, which triggered the bug.
     */
    public function testTemplateLoopAfterCount()
    {
        // Create test records
        $expectedTitles = [];
        for ($i = 1; $i <= 3; $i++) {
            $obj = new TestDataObject();
            $obj->Title = "Template Record $i";
            $obj->write();
            $expectedTitles[] = "Template Record $i";
        }

        // Get DataList
        $list = TestDataObject::get()->sort('ID');

        // Prime the list first to match the count-then-iterate template path
        $this->assertEquals(3, $list->count(), 'Count should return 3 before template rendering');

        // Create template with loop - SSViewer will call count() then iterate
        $template = '<% loop $TestList %>[{$Title}]<% end_loop %>';

        // Render using SSViewer
        $model = ArrayData::create(['TestList' => $list]);
        $viewer = SSViewer::fromString($template);
        $result = (string) $viewer->process($model);

        // Parse the result to extract titles
        preg_match_all('/\[([^\]]+)\]/', $result, $matches);
        $renderedTitles = $matches[1] ?? [];

        // Verify we got exactly 3 items in template output
        $this->assertEquals(
            $expectedTitles,
            $renderedTitles,
            'Template loop should render all items. Got: ' . json_encode($renderedTitles)
        );

        // Clean up
        foreach ($list as $obj) {
            $obj->delete();
        }
    }

    /**
     * Test direct query iteration after numRecords() - lower level test
     */
    public function testDirectQueryIterationAfterNumRecords()
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

        // Call numRecords() - this buffers all rows
        $this->assertEquals(3, $query->numRecords(), 'Direct query count should return 3');

        // Iterate - should still work even after numRecords()
        $iteratedIds = [];
        foreach ($query as $row) {
            $iteratedIds[] = (int) $row['ID'];
        }

        $this->assertEquals($ids, $iteratedIds, 'Direct query iteration should work after numRecords()');

        // Clean up
        TestDataObject::get()->filter('ID', $ids)->removeAll();
    }
}
