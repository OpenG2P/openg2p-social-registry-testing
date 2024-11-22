package main

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	_ "github.com/lib/pq"
	"github.com/opensearch-project/opensearch-go"
)

const (
	// OpenSearch configuration
	OPENSEARCH_HOST     = "localhost"
	OPENSEARCH_PORT     = 9201
	OPENSEARCH_USER     = "admin"
	OPENSEARCH_PASSWORD = "password"
	INDEX_NAME          = "loadtest_socialregistry.public.res_partner"

	// PostgreSQL configuration
	PG_HOST     = "localhost"
	PG_PORT     = 5432
	PG_USER     = "socialregistryuser"
	PG_PASSWORD = "password"
	PG_DATABASE = "socialregistrydb"

	// Chunk size for bulk operations
	CHUNK_SIZE = 10000 // Set chunk size to 10,000
)

// Transform dates and timestamps in the document
func transformDates(document map[string]interface{}) map[string]interface{} {
	// Add 'source_ts_ms' field with the current timestamp in milliseconds
	document["source_ts_ms"] = time.Now().UnixNano() / int64(time.Millisecond)

	// Format 'create_date' and 'write_date' to ISO format if needed
	for _, field := range []string{"create_date", "write_date"} {
		value, ok := document[field]
		if ok && value != nil {
			switch v := value.(type) {
			case time.Time:
				document[field] = v.Format(time.RFC3339)
			case string:
				// Try parsing the date string
				t, err := time.Parse("2006-01-02 15:04:05", v)
				if err != nil {
					t, err = time.Parse("2006-01-02", v)
					if err != nil {
						// If parsing fails, skip formatting this field
						continue
					}
				}
				document[field] = t.Format(time.RFC3339)
			}
		}
	}

	// Handle 'birthdate' and 'registration_date'
	for _, field := range []string{"birthdate", "registration_date"} {
		value, ok := document[field]
		if ok && value != nil {
			switch v := value.(type) {
			case time.Time:
				dateStr := v.Format("2006-01-02")
				document[field] = dateStr
			case string:
				t, err := time.Parse("2006-01-02", v)
				if err != nil {
					t, err = time.Parse("2006-01-02 15:04:05", v)
					if err != nil {
						// If parsing fails, skip formatting this field
						continue
					}
				}
				dateStr := t.Format("2006-01-02")
				document[field] = dateStr
			}
		}
	}

	// Convert 'date' field to milliseconds since epoch
	if value, ok := document["date"]; ok && value != nil {
		var t time.Time
		switch v := value.(type) {
		case time.Time:
			t = v
		case string:
			var err error
			t, err = time.Parse("2006-01-02", v)
			if err != nil {
				t, err = time.Parse("2006-01-02 15:04:05", v)
				if err != nil {
					// If parsing fails, remove the 'date' field
					delete(document, "date")
					break
				}
			}
		default:
			// If the type is unsupported, remove the 'date' field
			delete(document, "date")
			break
		}
		if !t.IsZero() {
			// Convert time to milliseconds since epoch
			epochMillis := t.UnixNano() / int64(time.Millisecond)
			document["date"] = epochMillis
		}
	}

	return document
}

// Fetch additional fields via joins
func performJoins(db *sql.DB, document map[string]interface{}) (map[string]interface{}, error) {
	// Join on 'district' to get 'district_name' and 'district_code'
	if districtID, ok := document["district"]; ok && districtID != nil {
		var districtName, districtCode string
		err := db.QueryRow("SELECT name, code FROM g2p_district WHERE id = $1", districtID).Scan(&districtName, &districtCode)
		if err != nil {
			if err == sql.ErrNoRows {
				document["district_name"] = ""
				document["district_code"] = ""
			} else {
				return document, err
			}
		} else {
			document["district_name"] = districtName
			document["district_code"] = districtCode
		}
	} else {
		document["district_name"] = ""
		document["district_code"] = ""
	}

	// Join on 'mandal' to get 'mandal_name' and 'mandal_code'
	if mandalID, ok := document["mandal"]; ok && mandalID != nil {
		var mandalName, mandalCode string
		err := db.QueryRow("SELECT name, code FROM g2p_mandal WHERE id = $1", mandalID).Scan(&mandalName, &mandalCode)
		if err != nil {
			if err == sql.ErrNoRows {
				document["mandal_name"] = ""
				document["mandal_code"] = ""
			} else {
				return document, err
			}
		} else {
			document["mandal_name"] = mandalName
			document["mandal_code"] = mandalCode
		}
	} else {
		document["mandal_name"] = ""
		document["mandal_code"] = ""
	}

	return document, nil
}

// Select appropriate timestamp for '@timestamp_gen'
func selectTimestamp(document map[string]interface{}) map[string]interface{} {
	for _, field := range []string{"write_date", "create_date", "source_ts_ms"} {
		if value, ok := document[field]; ok && value != nil {
			document["@timestamp_gen"] = value
			break
		}
	}
	return document
}

// Initialize the OpenSearch client
func createOpenSearchClient() (*opensearch.Client, error) {
	cfg := opensearch.Config{
		Addresses: []string{
			fmt.Sprintf("https://%s:%d", OPENSEARCH_HOST, OPENSEARCH_PORT),
		},
		Username: OPENSEARCH_USER,
		Password: OPENSEARCH_PASSWORD,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	client, err := opensearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Fetch a chunk of data from the PostgreSQL database
func fetchDataFromPostgres(db *sql.DB, offset int, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM res_partner OFFSET %d LIMIT %d;", offset, limit)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var data []map[string]interface{}

	for rows.Next() {
		// Create a slice of interfaces to hold each value
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// Create a map to hold column name to value mapping
		document := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Handle NULL values
			if val == nil {
				document[col] = nil
			} else {
				switch v := val.(type) {
				case []byte:
					document[col] = string(v)
				default:
					document[col] = v
				}
			}
		}

		// Perform joins to get additional fields
		document, err = performJoins(db, document)
		if err != nil {
			return nil, err
		}

		// Transform dates and select timestamp
		document = transformDates(document)
		document = selectTimestamp(document)

		data = append(data, document)
	}

	return data, nil
}

// Index a batch of documents into OpenSearch
func bulkIndexToOpenSearch(client *opensearch.Client, indexName string, data []map[string]interface{}) error {
	var buf bytes.Buffer

	for _, doc := range data {
		// Create the action metadata line
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
				"_id":    doc["id"],
			},
		}
		actionLine, err := json.Marshal(action)
		if err != nil {
			return fmt.Errorf("Error marshalling action: %v", err)
		}
		buf.Write(actionLine)
		buf.WriteByte('\n')

		// Create the source line
		sourceLine, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("Error marshalling doc: %v", err)
		}
		buf.Write(sourceLine)
		buf.WriteByte('\n')
	}

	// Perform the bulk request
	res, err := client.Bulk(bytes.NewReader(buf.Bytes()), client.Bulk.WithRefresh("true"))
	if err != nil {
		return fmt.Errorf("Error executing bulk request: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Bulk request failed with status %s", res.Status())
	}

	// Optionally parse the response for errors
	var bulkResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("Error parsing bulk response: %v", err)
	}

	// Check for errors in items
	if errors, ok := bulkResponse["errors"].(bool); ok && errors {
		// Iterate over items and print errors
		items := bulkResponse["items"].([]interface{})
		for _, item := range items {
			indexItem := item.(map[string]interface{})["index"].(map[string]interface{})
			if errorInfo, ok := indexItem["error"]; ok {
				// Log or handle error
				fmt.Printf("Error indexing document ID %v: %v\n", indexItem["_id"], errorInfo)
			}
		}
		return fmt.Errorf("Bulk request completed with errors")
	}

	fmt.Printf("Successfully indexed %d documents.\n", len(data))
	return nil
}

func main() {
	// Create OpenSearch client
	client, err := createOpenSearchClient()
	if err != nil {
		fmt.Printf("Error creating OpenSearch client: %v\n", err)
		return
	}

	// Connect to PostgreSQL
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("Error connecting to PostgreSQL: %v\n", err)
		return
	}
	defer db.Close()

	offset := 0
	totalProcessed := 0
	totalRecordsToProcess := 1000000

	for totalProcessed < totalRecordsToProcess {
		currentChunkSize := CHUNK_SIZE
		if totalRecordsToProcess-totalProcessed < CHUNK_SIZE {
			currentChunkSize = totalRecordsToProcess - totalProcessed
		}

		fmt.Printf("Fetching data from offset %d with limit %d...\n", offset, currentChunkSize)
		data, err := fetchDataFromPostgres(db, offset, currentChunkSize)
		if err != nil {
			fmt.Printf("Error fetching data from PostgreSQL: %v\n", err)
			break
		}

		if len(data) == 0 {
			fmt.Println("No more data to fetch.")
			break
		}

		fmt.Printf("Indexing %d documents...\n", len(data))
		err = bulkIndexToOpenSearch(client, INDEX_NAME, data)
		if err != nil {
			fmt.Printf("Error indexing data to OpenSearch: %v\n", err)
			break
		}

		offset += len(data)
		totalProcessed += len(data)

		if totalProcessed >= totalRecordsToProcess {
			fmt.Printf("Reached %d records.\n", totalRecordsToProcess)
			break
		}
	}

	fmt.Printf("Finished indexing %d documents.\n", totalProcessed)
}
