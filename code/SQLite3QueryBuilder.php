<?php

/**
 * Builds a SQL query string from a SQLExpression object
 * 
 * @package SQLite3
 */
class SQLite3QueryBuilder extends DBQueryBuilder {
	
	/**
	 * @param SQLInsert $query
	 * @param array $parameters
	 * @return string
	 */
	protected function buildInsertQuery(SQLInsert $query, array &$parameters) {
		// Multi-row insert requires SQLite specific syntax prior to 3.7.11
		// For backwards compatibility reasons include the "union all select" syntax
		
		$nl = $this->getSeparator();
		$into = $query->getInto();
		
		// Column identifiers
		$columns = $query->getColumns();
		
		// Build all rows
		$rowParts = array();
		foreach($query->getRows() as $row) {
			// Build all columns in this row
			$assignments = $row->getAssignments();
			// Join SET components together, considering parameters
			$parts = array();
			foreach($columns as $column) {
				// Check if this column has a value for this row
				if(isset($assignments[$column])) {
					// Assigment is a single item array, expand with a loop here
					foreach($assignments[$column] as $assignmentSQL => $assignmentParameters) {
						$parts[] = $assignmentSQL;
						$parameters = array_merge($parameters, $assignmentParameters);
						break;
					}
				} else {
					// This row is missing a value for a column used by another row
					$parts[] = '?';
					$parameters[] = null;
				}
			}
			$rowParts[] = implode(', ', $parts);
		}
		$columnSQL = implode(', ', $columns);
		$sql = "INSERT INTO {$into}{$nl}($columnSQL){$nl}SELECT " . implode("{$nl}UNION ALL SELECT ", $rowParts);
		
		return $sql;
	}

	/**
	 * Return the LIMIT clause ready for inserting into a query.
	 *
	 * @param SQLSelect $query The expression object to build from
	 * @param array $parameters Out parameter for the resulting query parameters
	 * @return string The finalised limit SQL fragment
	 */
	public function buildLimitFragment(SQLSelect $query, array &$parameters) {
		$nl = $this->getSeparator();

		// Ensure limit is given
		$limit = $query->getLimit();
		if(empty($limit)) return '';

		// For literal values return this as the limit SQL
		if( ! is_array($limit)) {
			return "{$nl}LIMIT $limit";
		}

		// Assert that the array version provides the 'limit' key
		if( ! array_key_exists('limit', $limit) || ($limit['limit'] !== null && ! is_numeric($limit['limit']))) {
			throw new InvalidArgumentException(
				'SQLite3QueryBuilder::buildLimitSQL(): Wrong format for $limit: '. var_export($limit, true)
			);
		}

		$clause = "{$nl}";
		if($limit['limit'] !== null) {
			$clause .= "LIMIT {$limit['limit']} ";
		} else {
			$clause .= "LIMIT -1 ";
		}
		
		if(isset($limit['start']) && is_numeric($limit['start']) && $limit['start'] !== 0) {
			$clause .= "OFFSET {$limit['start']}";
		}
		return $clause;
	}

}
