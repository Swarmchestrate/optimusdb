package app

import (
	"fmt"
	"strings"
)

/*
SQL Helper Functions - FIX #4

These functions help prevent SQL syntax errors like 'near "Alias": syntax error'
by properly quoting identifiers and escaping values.

Background:
- SQLite reserves words like "Alias", "Component", "Status", etc.
- Unquoted reserved words cause syntax errors
- Double-quoting identifiers makes them safe: SELECT "Alias", "Name" FROM ...
*/

// QuoteIdentifier wraps a column/table name in double quotes for SQLite
func QuoteIdentifier(name string) string {
	// Escape any double quotes in the name itself
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// QuoteIdentifiers quotes multiple identifiers for SELECT clauses
func QuoteIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = QuoteIdentifier(name)
	}
	return strings.Join(quoted, ", ")
}

// BuildSafeSelect constructs a SELECT query with quoted identifiers
// Example: BuildSafeSelect([]string{"Alias", "Name"}, "metadata", "Status = 'Active'")
// Returns: SELECT "Alias", "Name" FROM "metadata" WHERE Status = 'Active'
func BuildSafeSelect(columns []string, table string, whereClause string) string {
	quotedCols := QuoteIdentifiers(columns)
	quotedTable := QuoteIdentifier(table)

	if whereClause == "" {
		return fmt.Sprintf("SELECT %s FROM %s", quotedCols, quotedTable)
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", quotedCols, quotedTable, whereClause)
}

// SQLiteReservedWords lists common SQLite reserved words that need quoting
var SQLiteReservedWords = map[string]bool{
	"ABORT": true, "ACTION": true, "ADD": true, "AFTER": true, "ALL": true,
	"ALTER": true, "ALWAYS": true, "ANALYZE": true, "AND": true, "AS": true,
	"ASC": true, "ATTACH": true, "AUTOINCREMENT": true, "BEFORE": true, "BEGIN": true,
	"BETWEEN": true, "BY": true, "CASCADE": true, "CASE": true, "CAST": true,
	"CHECK": true, "COLLATE": true, "COLUMN": true, "COMMIT": true, "CONFLICT": true,
	"CONSTRAINT": true, "CREATE": true, "CROSS": true, "CURRENT": true, "DATABASE": true,
	"DEFAULT": true, "DEFERRABLE": true, "DEFERRED": true, "DELETE": true, "DESC": true,
	"DETACH": true, "DISTINCT": true, "DO": true, "DROP": true, "EACH": true,
	"ELSE": true, "END": true, "ESCAPE": true, "EXCEPT": true, "EXCLUDE": true,
	"EXCLUSIVE": true, "EXISTS": true, "EXPLAIN": true, "FAIL": true, "FILTER": true,
	"FIRST": true, "FOLLOWING": true, "FOR": true, "FOREIGN": true, "FROM": true,
	"FULL": true, "GENERATED": true, "GLOB": true, "GROUP": true, "GROUPS": true,
	"HAVING": true, "IF": true, "IGNORE": true, "IMMEDIATE": true, "IN": true,
	"INDEX": true, "INDEXED": true, "INITIALLY": true, "INNER": true, "INSERT": true,
	"INSTEAD": true, "INTERSECT": true, "INTO": true, "IS": true, "ISNULL": true,
	"JOIN": true, "KEY": true, "LAST": true, "LEFT": true, "LIKE": true,
	"LIMIT": true, "MATCH": true, "NATURAL": true, "NO": true, "NOT": true,
	"NOTHING": true, "NOTNULL": true, "NULL": true, "NULLS": true, "OF": true,
	"OFFSET": true, "ON": true, "OR": true, "ORDER": true, "OTHERS": true,
	"OUTER": true, "OVER": true, "PARTITION": true, "PLAN": true, "PRAGMA": true,
	"PRECEDING": true, "PRIMARY": true, "QUERY": true, "RAISE": true, "RANGE": true,
	"RECURSIVE": true, "REFERENCES": true, "REGEXP": true, "REINDEX": true, "RELEASE": true,
	"RENAME": true, "REPLACE": true, "RESTRICT": true, "RIGHT": true, "ROLLBACK": true,
	"ROW": true, "ROWS": true, "SAVEPOINT": true, "SELECT": true, "SET": true,
	"TABLE": true, "TEMP": true, "TEMPORARY": true, "THEN": true, "TIES": true,
	"TO": true, "TRANSACTION": true, "TRIGGER": true, "UNBOUNDED": true, "UNION": true,
	"UNIQUE": true, "UPDATE": true, "USING": true, "VACUUM": true, "VALUES": true,
	"VIEW": true, "VIRTUAL": true, "WHEN": true, "WHERE": true, "WINDOW": true,
	"WITH": true, "WITHOUT": true,
	// Additional words that commonly cause issues
	"ALIAS": true, "COMPONENT": true, "STATUS": true, "TYPE": true,
}

// IsReservedWord checks if a word is a SQLite reserved word
func IsReservedWord(word string) bool {
	return SQLiteReservedWords[strings.ToUpper(word)]
}

// ShouldQuote returns true if an identifier should be quoted
func ShouldQuote(identifier string) bool {
	// Quote if it's a reserved word or contains special characters
	if IsReservedWord(identifier) {
		return true
	}

	// Quote if it contains spaces, special chars, or starts with number
	for i, r := range identifier {
		if i == 0 && (r >= '0' && r <= '9') {
			return true
		}
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_') {
			return true
		}
	}

	return false
}
