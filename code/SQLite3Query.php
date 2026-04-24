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

    /**
     * Buffered rows fetched from the SQLite result handle.
     *
     * @var array<int, array<string, mixed>>
     */
    protected $rows = [];

    /**
     * Tracks whether the SQLite result handle has reached EOF.
     *
     * @var bool
     */
    protected $exhausted = false;

    /**
     * Shared cursor position across successive iterators.
     *
     * @var int
     */
    protected $currentIndex = 0;

    /**
     * Hook the result-set given into a Query class, suitable for use by framework.
     * @param SQLite3Connector $database The database object that created this query.
     * @param SQLite3Result $handle the internal sqlite3 handle that is points to the resultset.
     */
    public function __construct(SQLite3Connector $database, SQLite3Result $handle)
    {
        $this->database = $database;
        $this->handle = $handle;
    }

    public function __destruct()
    {
        if ($this->handle && $this->database->isActive()) {
            $this->handle->finalize();
        }
    }

    /**
     * @todo This looks terrible but there is no SQLite3::get_num_rows() implementation
     *
     * Drains any remaining rows into the buffer and returns the total count.
     */
    public function numRecords()
    {
        $this->loadAllRows();

        return count($this->rows);
    }

    public function getIterator(): Traversable
    {
        while (true) {
            // CRITICAL: Always check buffer FIRST before any handle operations
            // This ensures buffered rows are yielded even if handle is exhausted
            if (array_key_exists($this->currentIndex, $this->rows)) {
                $row = $this->rows[$this->currentIndex];
                $this->currentIndex++;
                yield $row;
                continue;
            }

            // Buffer is exhausted - now check if we can fetch more
            
            // If handle is not iterable, we're done
            if (!$this->handle->numColumns()) {
                $this->currentIndex = 0;
                return;
            }

            // If handle was previously exhausted, don't try to fetch again
            // (SQLite would restart from beginning, causing duplicates)
            if ($this->exhausted) {
                $this->currentIndex = 0;
                return;
            }

            // Try to fetch from handle
            $row = $this->handle->fetchArray(SQLITE3_ASSOC);
            if ($row === false) {
                // Mark as exhausted so we never try to fetch again
                $this->exhausted = true;
                $this->currentIndex = 0;
                return;
            }

            // Buffer the row and yield it
            $this->rows[] = $row;
            $this->currentIndex++;
            yield $row;
        }
    }

    public function rewind(): void
    {
        $this->currentIndex = 0;
    }

    /**
     * Drains the native handle into the local buffer until EOF.
     */
    protected function loadAllRows()
    {
        // Some queries are not iterable using fetchArray like CREATE statement.
        if ($this->exhausted || !$this->handle->numColumns()) {
            $this->exhausted = true;
            return;
        }

        while ($row = $this->handle->fetchArray(SQLITE3_ASSOC)) {
            $this->rows[] = $row;
        }

        // SQLite restarts from the first row after EOF, so never fetch again once exhausted.
        $this->exhausted = true;
    }
}
