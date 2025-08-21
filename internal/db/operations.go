package db

import (
	"database/sql"
	"fmt"
	"strings"
)

func BuildConnectionString(host string, port int, user, password, database, sslMode string) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, database, sslMode)
}

func BulkInsert(db *sql.DB, table string, records []map[string]interface{}, columns []string) error {
	if len(records) == 0 {
		return nil
	}

	placeholders := make([]string, len(records))
	values := make([]interface{}, 0, len(records)*len(columns))

	for i, record := range records {
		rowPlaceholders := make([]string, len(columns))
		for j, col := range columns {
			rowPlaceholders[j] = fmt.Sprintf("$%d", i*len(columns)+j+1)
			values = append(values, record[col])
		}
		placeholders[i] = "(" + strings.Join(rowPlaceholders, ", ") + ")"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := db.Exec(query, values...)
	return err
}

func InsertRecord(tx *sql.Tx, table string, data map[string]interface{}) error {
	columns := make([]string, 0, len(data))
	placeholders := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))

	i := 1
	for col, val := range data {
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		values = append(values, val)
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(query, values...)
	return err
}

func UpdateRecord(tx *sql.Tx, table string, data map[string]interface{}) error {
	setParts := make([]string, 0, len(data)-1)
	values := make([]interface{}, 0, len(data))
	
	var idValue interface{}
	i := 1

	for col, val := range data {
		if col == "id" {
			idValue = val
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s = $%d", col, i))
		values = append(values, val)
		i++
	}

	values = append(values, idValue)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d",
		table,
		strings.Join(setParts, ", "),
		i)

	_, err := tx.Exec(query, values...)
	return err
}

func DeleteRecord(tx *sql.Tx, table string, data map[string]interface{}) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", table)
	_, err := tx.Exec(query, data["id"])
	return err
}

func ExecuteQuery(db *sql.DB, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		record := make(map[string]interface{})
		for i, col := range columns {
			record[col] = values[i]
		}
		results = append(results, record)
	}

	return results, nil
}
