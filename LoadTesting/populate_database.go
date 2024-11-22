package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"

	_ "github.com/lib/pq"
)

// Configuration
const (
	OUTPUT_FOLDER            = "data"
	PARTNERS_FOLDER          = OUTPUT_FOLDER + "/partners"
	REG_IDS_FOLDER           = OUTPUT_FOLDER + "/reg_ids"
	GROUPS_FOLDER            = OUTPUT_FOLDER + "/groups"
	GROUP_IDS_FOLDER         = OUTPUT_FOLDER + "/group_reg_ids"
	GROUP_MEMBERSHIPS_FOLDER = OUTPUT_FOLDER + "/group_memberships"
)

// Database connection settings
const (
	dbname   = "socialregistrydb"
	user     = "socialregistryuser"
	password = "xwfJhfI9tL"
	host     = "172.29.8.235"
	port     = "5432"
)

func main() {
	// Connect to the PostgreSQL database
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Check the connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping the database: %v", err)
	}

	// Get stats before seeding
	fmt.Println("-------------------- Before seeding --------------------")
	getStats(db)

	// Seed the database
	fmt.Println("Seeding Partners...")
	seedFolder(db, PARTNERS_FOLDER)

	fmt.Println("Seeding Registration IDs...")
	seedFolder(db, REG_IDS_FOLDER)

	fmt.Println("Seeding Groups...")
	seedFolder(db, GROUPS_FOLDER)

	fmt.Println("Seeding Group IDs...")
	seedFolder(db, GROUP_IDS_FOLDER)

	fmt.Println("Seeding Group Memberships...")
	seedFolder(db, GROUP_MEMBERSHIPS_FOLDER)

	// Get stats after seeding
	fmt.Println("-------------------- After seeding --------------------")
	getStats(db)
}

// Function to execute SQL from a file
func executeSQLFile(db *sql.DB, filePath string) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read file %s: %v", filePath, err)
		return
	}

	_, err = db.Exec(string(content))
	if err != nil {
		log.Printf("Error executing %s: %v", filePath, err)
	} else {
		log.Printf("Successfully executed %s", filePath)
	}
}

// Function to iterate over all files in a folder and execute SQL
func seedFolder(db *sql.DB, folderPath string) {
	files, err := ioutil.ReadDir(folderPath)
	if err != nil {
		log.Printf("Failed to read directory %s: %v", folderPath, err)
		return
	}

	sortedFiles := make([]string, 0, len(files))
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sql" {
			sortedFiles = append(sortedFiles, filepath.Join(folderPath, file.Name()))
		}
	}

	sort.Strings(sortedFiles)

	for _, filePath := range sortedFiles {
		executeSQLFile(db, filePath)
	}
}

// Function to get database and table stats
func getStats(db *sql.DB) {
	// Get the total db size
	var dbSize string
	err := db.QueryRow("SELECT pg_size_pretty(pg_database_size($1));", dbname).Scan(&dbSize)
	if err != nil {
		log.Printf("Failed to get database size: %v", err)
	} else {
		fmt.Printf("Database size: %s\n", dbSize)
	}

	// Get total table size for each table
	tables := []string{"res_partner", "g2p_reg_id", "g2p_group_membership"}
	for _, table := range tables {
		var tableSize string
		err = db.QueryRow(fmt.Sprintf("SELECT pg_size_pretty(pg_total_relation_size('%s'));", table)).Scan(&tableSize)
		if err != nil {
			log.Printf("Failed to get size for table %s: %v", table, err)
		} else {
			fmt.Printf("%s table size: %s\n", table, tableSize)
		}
	}
}
