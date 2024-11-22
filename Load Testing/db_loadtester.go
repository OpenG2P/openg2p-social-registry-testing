package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// Database connection parameters
	const (
		host     = "172.29.8.235"
		port     = 5432
		user     = "socialregistryuser"
		password = "password"
		dbname   = "socialregistrydb"
	)

	// Build the connection string
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	// Open the database connection
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Successfully connected!")

	// Number of concurrent queries
	n := 400 // Set n to your desired number

	// Define the base query
	baseQuery := `
SELECT "res_partner"."id", "res_partner"."name", "res_partner"."address", "res_partner"."phone", "res_partner"."birthdate", "res_partner"."registration_date", "res_partner"."disabled"
FROM "res_partner"
WHERE (((("res_partner"."active" = true)
        AND ("res_partner"."is_registrant" = TRUE))
        AND ("res_partner"."is_group" IS NULL OR "res_partner"."is_group" = FALSE))
        AND ("res_partner"."complete_name"::text ILIKE $1))
    AND (("res_partner"."partner_share" IS NULL OR "res_partner"."partner_share" = FALSE)
        OR (("res_partner"."company_id" IN (1)) OR "res_partner"."company_id" IS NULL))
ORDER BY "res_partner"."complete_name" ASC, "res_partner"."id" DESC
LIMIT 80;
`

	// Parameters for the queries
	names := []string{
		"%HARVEY%",
		"%DOE%",
		"%JANE%",
		"%BROWN%",
		"%LISA%",
		"%JOHNSON%",
		"%DAVIS%",
		"%MILLER%",
		"%LAURA%",
		"%ANDERSON%",
	}

	// Ensure we have enough names
	for len(names) < n {
		names = append(names, "%UNKNOWN%")
	}

	overallStartTime := time.Now()
	fmt.Printf("Overall execution started at: %s\n", overallStartTime.Format(time.RFC3339))

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		nameParam := names[i]
		go func(name string) {
			defer wg.Done()
			// Execute the query with parameter
			rows, err := db.Query(baseQuery, name)
			if err != nil {
				log.Println("Query error:", err)
				return
			}
			defer rows.Close()

			// Process the rows
			for rows.Next() {
				var (
					id               int
					name             sql.NullString
					address          sql.NullString
					phone            sql.NullString
					birthdate        sql.NullTime
					registrationDate sql.NullTime
					disabled         sql.NullBool
				)

				err = rows.Scan(&id, &name, &address, &phone, &birthdate, &registrationDate, &disabled)
				if err != nil {
					log.Println("Row scan error:", err)
					return
				}

			}

			if err = rows.Err(); err != nil {
				log.Println("Rows error:", err)
			}
		}(nameParam)
	}

	wg.Wait()

	overallEndTime := time.Now()
	overallDuration := overallEndTime.Sub(overallStartTime)
	fmt.Printf("Overall execution ended at: %s\n", overallEndTime.Format(time.RFC3339))
	fmt.Printf("Total execution time: %v\n", overallDuration)

	fmt.Println("All queries completed.")
}
