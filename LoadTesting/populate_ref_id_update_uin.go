package main

import (
    "database/sql"
    "fmt"
    "log"
    "sync"
    "time"

    "github.com/lib/pq"
)

type DBConfig struct {
    Host     string
    Port     int
    User     string
    Password string
    Database string
}

var CONFIG = map[string]DBConfig{
    "SOURCE_DB": {
        Host:     "localhost",
        Port:     5433,
        User:     "postgres",
        Password: "password",
        Database: "mosip_kernel",
    },
    "TARGET_DB": {
        Host:     "localhost",
        Port:     5432,
        User:     "socialregistryuser",
        Password: "password",
        Database: "socialregistrydb",
    },
}

func getDBSession(config DBConfig) (*sql.DB, error) {
    connStr := fmt.Sprintf(
        "host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
        config.Host, config.Port, config.User, config.Password, config.Database,
    )
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to database: %w", err)
    }
    // Verify the connection is good
    if err := db.Ping(); err != nil {
        return nil, fmt.Errorf("failed to ping database: %w", err)
    }
    return db, nil
}

func getUnusedUINs(config DBConfig, batchSize int) ([]string, error) {
    db, err := getDBSession(config)
    if err != nil {
        return nil, err
    }
    defer db.Close()

    query := `SELECT uin FROM kernel.uin WHERE uin_status = 'UNUSED' LIMIT $1`
    rows, err := db.Query(query, batchSize)
    if err != nil {
        return nil, fmt.Errorf("failed to execute query: %w", err)
    }
    defer rows.Close()

    var uins []string
    for rows.Next() {
        var uin string
        if err := rows.Scan(&uin); err != nil {
            return nil, fmt.Errorf("failed to scan row: %w", err)
        }
        uins = append(uins, uin)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("row iteration error: %w", err)
    }
    return uins, nil
}

func getResPartnersWithoutRefID(config DBConfig, batchSize int) ([]int, error) {
    db, err := getDBSession(config)
    if err != nil {
        return nil, err
    }
    defer db.Close()

    query := `SELECT id FROM res_partner WHERE ref_id IS NULL LIMIT $1`
    rows, err := db.Query(query, batchSize)
    if err != nil {
        return nil, fmt.Errorf("failed to execute query: %w", err)
    }
    defer rows.Close()

    var ids []int
    for rows.Next() {
        var id int
        if err := rows.Scan(&id); err != nil {
            return nil, fmt.Errorf("failed to scan row: %w", err)
        }
        ids = append(ids, id)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("row iteration error: %w", err)
    }
    return ids, nil
}

func updateRefIDs(db *sql.DB, resPartnerIDs []int, uins []string) error {
    if len(resPartnerIDs) != len(uins) {
        return fmt.Errorf("length of resPartnerIDs and uins must be equal")
    }

    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer func() {
        if err != nil {
            tx.Rollback()
        }
    }()

    query := `
        WITH data(id, ref_id) AS (
            SELECT unnest($1::int[]), unnest($2::text[])
        )
        UPDATE res_partner rp
        SET ref_id = data.ref_id
        FROM data
        WHERE rp.id = data.id;
    `
    _, err = tx.Exec(query, pq.Array(resPartnerIDs), pq.Array(uins))
    if err != nil {
        tx.Rollback()
        return fmt.Errorf("failed to execute update query: %w", err)
    }

    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }
    return nil
}

func updateUINStatus(db *sql.DB, uins []string) error {
    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer func() {
        if err != nil {
            tx.Rollback()
        }
    }()

    query := `
        UPDATE kernel.uin
        SET uin_status = 'ASSIGNED'
        WHERE uin = ANY($1)
    `
    _, err = tx.Exec(query, pq.Array(uins))
    if err != nil {
        tx.Rollback()
        return fmt.Errorf("failed to execute update query: %w", err)
    }

    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }
    return nil
}

func processChunk(
    resPartnerIDsChunk []int,
    uinsChunk []string,
    sourceDBConfig, targetDBConfig DBConfig,
    resultChan chan<- int,
) {
    startTime := time.Now()
    threadName := fmt.Sprintf("Goroutine-%d", time.Now().UnixNano())
    fmt.Printf("Thread %s started at %s\n", threadName, startTime.Format("2006-01-02 15:04:05"))

    defer func() {
        endTime := time.Now()
        fmt.Printf("Thread %s ended at %s\n", threadName, endTime.Format("2006-01-02 15:04:05"))
    }()

    sourceDB, err := getDBSession(sourceDBConfig)
    if err != nil {
        log.Printf("Exception in processChunk (sourceDB): %v\n", err)
        resultChan <- 0
        return
    }
    defer sourceDB.Close()

    targetDB, err := getDBSession(targetDBConfig)
    if err != nil {
        log.Printf("Exception in processChunk (targetDB): %v\n", err)
        resultChan <- 0
        return
    }
    defer targetDB.Close()

    if err := updateRefIDs(targetDB, resPartnerIDsChunk, uinsChunk); err != nil {
        log.Printf("Failed to update ref_ids: %v\n", err)
        resultChan <- 0
        return
    }

    if err := updateUINStatus(sourceDB, uinsChunk); err != nil {
        log.Printf("Failed to update UIN status: %v\n", err)
        resultChan <- 0
        return
    }

    resultChan <- len(resPartnerIDsChunk)
}

func main() {
    TOTAL_RECORDS_TO_PROCESS := 40000
    BATCH_SIZE := 40000
    SUB_BATCH_SIZE := 10000

    totalProcessed := 0
    startTime := time.Now()

    for {
        remainingRecords := TOTAL_RECORDS_TO_PROCESS - totalProcessed
        currentBatchSize := BATCH_SIZE
        if remainingRecords < BATCH_SIZE {
            currentBatchSize = remainingRecords
        }

        resPartnerIDs, err := getResPartnersWithoutRefID(CONFIG["TARGET_DB"], currentBatchSize)
        if err != nil {
            log.Printf("Error fetching res_partner_ids: %v\n", err)
            break
        }
        if len(resPartnerIDs) == 0 {
            fmt.Println("No more res_partner records without ref_id")
            break
        }

        uins, err := getUnusedUINs(CONFIG["SOURCE_DB"], len(resPartnerIDs))
        if err != nil {
            log.Printf("Error fetching UINs: %v\n", err)
            break
        }
        if len(uins) == 0 {
            fmt.Println("No more UINs available")
            break
        }

        actualBatchSize := len(resPartnerIDs)
        if len(uins) < actualBatchSize {
            actualBatchSize = len(uins)
        }
        fmt.Printf("Processing batch of %d records\n", actualBatchSize)

        resPartnerIDs = resPartnerIDs[:actualBatchSize]
        uins = uins[:actualBatchSize]

        type chunk struct {
            resPartnerIDsChunk []int
            uinsChunk          []string
        }
        var chunks []chunk
        for i := 0; i < actualBatchSize; i += SUB_BATCH_SIZE {
            end := i + SUB_BATCH_SIZE
            if end > actualBatchSize {
                end = actualBatchSize
            }
            chunks = append(chunks, chunk{
                resPartnerIDsChunk: resPartnerIDs[i:end],
                uinsChunk:          uins[i:end],
            })
        }

        var wg sync.WaitGroup
        resultChan := make(chan int, len(chunks))
        for _, c := range chunks {
            wg.Add(1)
            go func(c chunk) {
                defer wg.Done()
                processChunk(c.resPartnerIDsChunk, c.uinsChunk, CONFIG["SOURCE_DB"], CONFIG["TARGET_DB"], resultChan)
            }(c)
        }

        wg.Wait()
        close(resultChan)

        for processed := range resultChan {
            totalProcessed += processed
            fmt.Printf("Processed %d records in thread\n", processed)
        }

        elapsedTime := time.Since(startTime).Seconds()
        rate := float64(totalProcessed) / elapsedTime
        fmt.Printf(
            "Progress: %d/%d records processed (%.2f records/second)\n",
            totalProcessed, TOTAL_RECORDS_TO_PROCESS, rate,
        )

        if totalProcessed >= TOTAL_RECORDS_TO_PROCESS {
            fmt.Println("Reached target number of records to process")
            break
        }
    }

    fmt.Printf("Processing completed. Total records processed: %d\n", totalProcessed)
}
