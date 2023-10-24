<?php

namespace SilverStripe\SQLite;

use SilverStripe\ORM\Connect\Query;
use SQLite3Result;
use Traversable;

/**
 * A result-set from a SQLite3 database.
 */
class SQLite3Query extends Query
{

    /**
     * The SQLite3Connector object that created this result set.
     *
     * @var SQLite3Connector
     */
    protected $database;

    /**
     * The internal sqlite3 handle that points to the result set.
     *
     * @var SQLite3Result
     */
    protected $handle;

    private int $count = 0;

    /**
     * Hook the result-set given into a Query class, suitable for use by framework.
     * @param SQLite3Connector $database The database object that created this query.
     * @param SQLite3Result $handle the internal sqlite3 handle that is points to the resultset.
     */
    public function __construct(SQLite3Connector $database, SQLite3Result $handle)
    {
        $this->database = $database;
        $this->handle = $handle;
        // Count early to make sure we don't interfere with the generator and rewind operation
        $this->count = $this->countRecords();
    }

    public function __destruct()
    {
        if ($this->handle) {
            $this->handle->finalize();
        }
    }

    public function numRecords()
    {
        return $this->count;
    }

    /**
     * @todo This looks terrible but there is no SQLite3::get_num_rows() implementation
     */
    private function countRecords()
    {
        // Some queries are not iterable using fetchArray like CREATE statement
        if (!$this->handle->numColumns()) {
            return 0;
        }

        $this->handle->reset();
        $c = 0;
        while ($this->handle->fetchArray()) {
            $c++;
        }
        $this->handle->reset();
        return $c;
    }

    public function getIterator(): Traversable
    {
        while ($data = $this->handle->fetchArray(SQLITE3_ASSOC)) {
            yield $data;
        }
    }
}
