<?php

/**
 * Builds a SQL query string from a SQLExpression object
 * 
 * @package SQLite3
 */
class SQLite3QueryBuilder extends DBQueryBuilder {
	
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
}
