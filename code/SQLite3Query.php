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
     * Buffered rows. Acts as a cache to avoid double-fetching and to allow re-iteration.
     *
     * @var array
     */
    private array $rows = [];

    /**
     * Whether the native handle has reached EOF. Prevents SQLite from wrapping back to the first row.
     *
     * @var bool
     */
    private bool $exhausted = false;

    /**
     * Current iteration position across successive iterators.
     *
     * @var int
     */
    private int $currentIndex = 0;

    /**
     * @param SQLite3Connector $database
     * @param SQLite3Result $handle
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

    /**
     * Drains the native handle into the local buffer until EOF.
     */
    private function loadAllRows(): void
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

    /**
     * Yields rows from the buffer first, then fetches incrementally from the native handle.
     * Fetched rows are buffered to allow re-iteration and to avoid double-fetch bugs.
     */
    public function getIterator(): Traversable
    {
        while (true) {
            if (array_key_exists($this->currentIndex, $this->rows)) {
                $row = $this->rows[$this->currentIndex];
                $this->currentIndex++;
                yield $row;
                continue;
            }

            if ($this->exhausted || !$this->handle->numColumns()) {
                $this->exhausted = true;
                $this->currentIndex = 0;
                return;
            }

            $row = $this->handle->fetchArray(SQLITE3_ASSOC);
            if ($row === false) {
                // SQLite restarts from the first row after EOF, so never fetch again once exhausted.
                $this->exhausted = true;
                $this->currentIndex = 0;
                return;
            }

            $this->rows[] = $row;
            $this->currentIndex++;
            yield $row;
        }
    }

    public function rewind(): void
    {
        $this->currentIndex = 0;
    }
}
