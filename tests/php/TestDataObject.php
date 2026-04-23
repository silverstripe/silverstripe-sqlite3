<?php

namespace SilverStripe\SQLite\Tests;

use SilverStripe\Dev\TestOnly;
use SilverStripe\ORM\DataObject;

/**
 * Test DataObject for SQLite3 Query Iterator tests
 */
class TestDataObject extends DataObject implements TestOnly
{
    private static $table_name = 'SQLite3QueryIteratorTest_DataObject';

    private static $db = [
        'Title' => 'Varchar(255)',
    ];
}
