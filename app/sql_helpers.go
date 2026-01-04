package app

import (
	"fmt"
	"strings"
)

/*
SQL HELPERS - Safe Query Construction for SQLite

This file provides utilities to prevent "near X: syntax error" when using
SQLite reserved words as column/table names.

PROBLEM: SQLite has reserved words like Alias, Component, Status, Type, etc.
Using these unquoted in queries causes syntax errors.

SOLUTION: Wrap identifiers in double quotes to make them safe.

Usage:
  import "optimusdb/app"

  // Quote a single identifier
  quotedAlias := app.QuoteIdentifier("Alias")
  // Returns: "Alias"

  // Build safe SELECT query
  query := app.BuildSafeSelect(
      []string{"Alias", "Component", "Status"},
      "metadata",
      "Status = 'Active'",
  )
  // Returns: SELECT "Alias", "Component", "Status" FROM "metadata" WHERE Status = 'Active'
*/

// QuoteIdentifier wraps an identifier in double quotes for SQLite
func QuoteIdentifier(name string) string {
	// Remove any existing quotes first
	name = strings.Trim(name, "\"")
	return fmt.Sprintf("\"%s\"", name)
}

// QuoteIdentifiers wraps multiple identifiers in double quotes
func QuoteIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = QuoteIdentifier(name)
	}
	return strings.Join(quoted, ", ")
}

// BuildSafeSelect constructs a safe SELECT query with quoted identifiers
//
// Example:
//
//	query := BuildSafeSelect(
//	    []string{"Alias", "Component"},
//	    "metadata",
//	    "Status = 'Active'",
//	)
//	// Returns: SELECT "Alias", "Component" FROM "metadata" WHERE Status = 'Active'
func BuildSafeSelect(columns []string, table string, whereClause string) string {
	quotedCols := QuoteIdentifiers(columns)
	quotedTable := QuoteIdentifier(table)

	if whereClause == "" {
		return fmt.Sprintf("SELECT %s FROM %s", quotedCols, quotedTable)
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", quotedCols, quotedTable, whereClause)
}

// SQLiteReservedWords contains all SQLite reserved keywords that require quoting
var SQLiteReservedWords = map[string]bool{
	"ABORT":             true,
	"ACTION":            true,
	"ADD":               true,
	"AFTER":             true,
	"ALL":               true,
	"ALTER":             true,
	"ALWAYS":            true,
	"ANALYZE":           true,
	"AND":               true,
	"AS":                true,
	"ASC":               true,
	"ATTACH":            true,
	"AUTOINCREMENT":     true,
	"BEFORE":            true,
	"BEGIN":             true,
	"BETWEEN":           true,
	"BY":                true,
	"CASCADE":           true,
	"CASE":              true,
	"CAST":              true,
	"CHECK":             true,
	"COLLATE":           true,
	"COLUMN":            true,
	"COMMIT":            true,
	"CONFLICT":          true,
	"CONSTRAINT":        true,
	"CREATE":            true,
	"CROSS":             true,
	"CURRENT":           true,
	"CURRENT_DATE":      true,
	"CURRENT_TIME":      true,
	"CURRENT_TIMESTAMP": true,
	"DATABASE":          true,
	"DEFAULT":           true,
	"DEFERRABLE":        true,
	"DEFERRED":          true,
	"DELETE":            true,
	"DESC":              true,
	"DETACH":            true,
	"DISTINCT":          true,
	"DO":                true,
	"DROP":              true,
	"EACH":              true,
	"ELSE":              true,
	"END":               true,
	"ESCAPE":            true,
	"EXCEPT":            true,
	"EXCLUDE":           true,
	"EXCLUSIVE":         true,
	"EXISTS":            true,
	"EXPLAIN":           true,
	"FAIL":              true,
	"FILTER":            true,
	"FIRST":             true,
	"FOLLOWING":         true,
	"FOR":               true,
	"FOREIGN":           true,
	"FROM":              true,
	"FULL":              true,
	"GENERATED":         true,
	"GLOB":              true,
	"GROUP":             true,
	"GROUPS":            true,
	"HAVING":            true,
	"IF":                true,
	"IGNORE":            true,
	"IMMEDIATE":         true,
	"IN":                true,
	"INDEX":             true,
	"INDEXED":           true,
	"INITIALLY":         true,
	"INNER":             true,
	"INSERT":            true,
	"INSTEAD":           true,
	"INTERSECT":         true,
	"INTO":              true,
	"IS":                true,
	"ISNULL":            true,
	"JOIN":              true,
	"KEY":               true,
	"LAST":              true,
	"LEFT":              true,
	"LIKE":              true,
	"LIMIT":             true,
	"MATCH":             true,
	"MATERIALIZED":      true,
	"NATURAL":           true,
	"NO":                true,
	"NOT":               true,
	"NOTHING":           true,
	"NOTNULL":           true,
	"NULL":              true,
	"NULLS":             true,
	"OF":                true,
	"OFFSET":            true,
	"ON":                true,
	"OR":                true,
	"ORDER":             true,
	"OTHERS":            true,
	"OUTER":             true,
	"OVER":              true,
	"PARTITION":         true,
	"PLAN":              true,
	"PRAGMA":            true,
	"PRECEDING":         true,
	"PRIMARY":           true,
	"QUERY":             true,
	"RAISE":             true,
	"RANGE":             true,
	"RECURSIVE":         true,
	"REFERENCES":        true,
	"REGEXP":            true,
	"REINDEX":           true,
	"RELEASE":           true,
	"RENAME":            true,
	"REPLACE":           true,
	"RESTRICT":          true,
	"RETURNING":         true,
	"RIGHT":             true,
	"ROLLBACK":          true,
	"ROW":               true,
	"ROWS":              true,
	"SAVEPOINT":         true,
	"SELECT":            true,
	"SET":               true,
	"TABLE":             true,
	"TEMP":              true,
	"TEMPORARY":         true,
	"THEN":              true,
	"TIES":              true,
	"TO":                true,
	"TRANSACTION":       true,
	"TRIGGER":           true,
	"UNBOUNDED":         true,
	"UNION":             true,
	"UNIQUE":            true,
	"UPDATE":            true,
	"USING":             true,
	"VACUUM":            true,
	"VALUES":            true,
	"VIEW":              true,
	"VIRTUAL":           true,
	"WHEN":              true,
	"WHERE":             true,
	"WINDOW":            true,
	"WITH":              true,
	"WITHOUT":           true,

	// Common words that are often problematic (even if not strictly reserved)
	"Alias":     true,
	"Component": true,
	"Status":    true,
	"Type":      true,
	"Name":      true,
}

// IsReservedWord checks if a word is a SQLite reserved keyword
func IsReservedWord(word string) bool {
	return SQLiteReservedWords[strings.ToUpper(word)]
}

// ShouldQuote determines if an identifier should be quoted
// Returns true if the identifier is a reserved word or contains special characters
func ShouldQuote(identifier string) bool {
	// Always quote if it's a reserved word
	if IsReservedWord(identifier) {
		return true
	}

	// Quote if it contains spaces or special characters
	for _, char := range identifier {
		if char == ' ' || char == '-' || char == '.' || char == ':' {
			return true
		}
	}

	return false
}
