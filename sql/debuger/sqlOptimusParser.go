package debuger

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"strings"
)

/**
	THe aim is to expose optimusdDB as a SQL-compatible database
	The idea for this is an abstraction layer
	SQL input:
    	Accept SQL query strings SELECT, INSERT, DELETE, or UPDATE.
	Parse the SQL:
    	Use the sqlparser library to convert the SQL query into a structured Go representation.
	Translates to OrbitDB Operations:
	    Implement functions that map parsed SQL components to OrbitDB-compatible operations.
	Executes the Query:
    	Run optimusDB operations (like Query(), Put(), Delete()) based on the translated query.


	Some limitation to consider....
		Condition Parsing: Properly parse the WHERE clause to construct OrbitDB filters.
		Data Mapping: OrbitDB uses JSON-like structures, so ensure SQL table concepts align with document-based storage.
		Limitations: Complex SQL operations like joins, aggregations (GROUP BY), and transactions are not natively supported.

*/
// Mock OrbitDB client for demonstration
type OrbitDBClient struct {
	data map[string]map[string]interface{} // Simulate key-value store
}

func NewOrbitDBClient() *OrbitDBClient {
	return &OrbitDBClient{
		data: make(map[string]map[string]interface{}),
	}
}

func (c *OrbitDBClient) Put(key string, value map[string]interface{}) {
	c.data[key] = value
	fmt.Printf("Inserted/Updated record: %s -> %v\n", key, value)
}

func (c *OrbitDBClient) GetAll() map[string]map[string]interface{} {
	return c.data
}

func (c *OrbitDBClient) Get(key string) (map[string]interface{}, bool) {
	value, exists := c.data[key]
	return value, exists
}

// SQL to OrbitDB Translator
type SQLToOrbitDB struct {
	orbitDB *OrbitDBClient
}

func NewSQLToOrbitDB(orbitDB *OrbitDBClient) *SQLToOrbitDB {
	return &SQLToOrbitDB{
		orbitDB: orbitDB,
	}
}

func (s *SQLToOrbitDB) handleInsert(stmt *sqlparser.Insert) error {
	tableName := stmt.Table.Name.String()

	// Extract column names
	columnNames := []string{}
	for _, col := range stmt.Columns {
		columnNames = append(columnNames, col.String())
	}

	// Extract row values
	for _, row := range stmt.Rows.(sqlparser.Values) {
		record := make(map[string]interface{})
		for i, value := range row {
			column := columnNames[i]
			record[column] = string(value.(*sqlparser.SQLVal).Val)
		}

		// Generate a unique key (assumes a column named 'id' for the key)
		key := fmt.Sprintf("%s/%s", tableName, record["id"])
		s.orbitDB.Put(key, record)
	}

	return nil
}

// ExecuteSQLOptimus
func (s *SQLToOrbitDB) ExecuteSQLOptimus(sql string) (interface{}, error) {
	// Step 1: Parse the SQL query
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %v", err)
	}

	// Step 2: Handle the parsed SQL statement based on its type
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return nil, s.handleInsert(stmt) // Handle INSERT logic

	case *sqlparser.Select:
		return s.handleSelect(stmt) // Handle SELECT logic

	case *sqlparser.Update:
		return nil, s.handleUpdate(stmt) // Handle UPDATE logic

	default:
		return nil, fmt.Errorf("unsupported SQL statement type")
	}
}

func (s *SQLToOrbitDB) handleSelect(stmt *sqlparser.Select) (interface{}, error) {
	tableName := stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	// Simulate filtering
	data := s.orbitDB.GetAll()
	results := []map[string]interface{}{}
	fmt.Printf("Data in table '%s':\n", tableName)
	for key, value := range data {
		if strings.HasPrefix(key, tableName+"/") {
			fmt.Printf("Key: %s, Value: %v\n", key, value)
			results = append(results, value)
		}
	}

	return results, nil
}

func (s *SQLToOrbitDB) handleUpdate(stmt *sqlparser.Update) error {
	tableName := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	// Extract updates
	updates := make(map[string]interface{})
	for _, expr := range stmt.Exprs {
		column := expr.Name.Name.String()
		value := string(expr.Expr.(*sqlparser.SQLVal).Val)
		updates[column] = value
	}

	// Apply updates to the simulated OrbitDB (assumes a column named 'id' for key identification)
	idValue := ""
	if comparisonExpr, ok := stmt.Where.Expr.(*sqlparser.ComparisonExpr); ok {
		idValue = string(comparisonExpr.Right.(*sqlparser.SQLVal).Val)
	}

	key := fmt.Sprintf("%s/%s", tableName, idValue)
	if record, exists := s.orbitDB.Get(key); exists {
		for k, v := range updates {
			record[k] = v
		}
		s.orbitDB.Put(key, record)
	} else {
		return fmt.Errorf("record not found for key: %s", key)
	}

	return nil
}
