import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List
import time

CONFIG = {
    'SOURCE_DB': {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'password',
        'database': 'database'
    },
    'TARGET_DB': {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'password',
        'database': 'database'
    }
}

# CREATE DB SESSION
def get_db_session(config: Dict) -> psycopg2.extensions.connection:
    
    try:
        return psycopg2.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )

    except Exception as e:
        print(f"Failed to connect to database: {e}")
        raise

# GET UNUSED uin (uin_status = UNUSED) FROM kernel.uin
def get_unused_uins(session: psycopg2.extensions.connection, BATCH_SIZE: int) -> List[tuple]:
    
    with session.cursor() as cur:
        cur.execute("""
            SELECT uin 
            FROM kernel.uin
            WHERE uin_status = 'UNUSED'
            LIMIT %s
        """, (BATCH_SIZE,))

        return cur.fetchall()

# BULK INSERT uin AS ref_id INTO res_partner
def insert_ref_ids(session: psycopg2.extensions.connection, uin_batch: List[tuple]) -> bool:

    try:
        with session.cursor() as cur:
            values = [(uin[0],) for uin in uin_batch]
            execute_values(cur, """
                INSERT INTO res_partner (ref_id)
                VALUES %s
            """, values)
        session.commit()

        return True

    except Exception as e:
        session.rollback()
        print(f"Failed to insert ref_ids: {e}")
        return False

# UPDATE uin_status = ISSUED IN kernel.uin
def update_uin_status(session: psycopg2.extensions.connection, uin_batch: List[tuple]) -> bool:

    try:
        with session.cursor() as cur:
            issued_values = [uin[0] for uin in uin_batch]
            cur.execute("""
                UPDATE kernel.uin 
                SET uin_status = 'ISSUED'
                WHERE uin = ANY(%s)
            """, (issued_values,))
        session.commit()

        return True

    except Exception as e:
        session.rollback()
        print(f"Failed to update UIN status: {e}")
        return False


def main():
    source_session = None
    target_session = None
    
    TOTAL_RECORDS_TO_PROCESS = 1000
    BATCH_SIZE = 10
    
    try:
        source_session = get_db_session(CONFIG['SOURCE_DB'])
        target_session = get_db_session(CONFIG['TARGET_DB'])
        
        offset = 0
        total_processed = 0
        start_time = time.time()
        
        while True:
            # Calculate current batch size
            remaining_records = TOTAL_RECORDS_TO_PROCESS - total_processed
            current_batch_size = min(BATCH_SIZE, remaining_records)
            
            uin_batch = get_unused_uins(source_session, current_batch_size)
            
            # No more records to process in db
            if not uin_batch:
                print("No more records available to process")
                break
                
            actual_batch_size = len(uin_batch)
            print(f"Processing batch of {actual_batch_size} records (offset: {offset})")

            # Inserting ref_ids and updating uin_status
            if insert_ref_ids(target_session, uin_batch):
                if update_uin_status(source_session, uin_batch):
                    total_processed += actual_batch_size
                    print(f"Successfully processed {actual_batch_size} records")
                else:
                    print("Failed to update uin_status, rolling back batch")
                    continue
            else:
                print("Failed to insert ref_ids, skipping batch")
                continue
            
            offset += actual_batch_size
            
            # Elapsed and rate statistics
            elapsed_time = time.time() - start_time
            rate = total_processed / elapsed_time if elapsed_time > 0 else 0
            print(f"Progress: {total_processed}/{TOTAL_RECORDS_TO_PROCESS} records processed ({rate:.2f} records/second)")
            
            # Break if completed processing required records
            if total_processed >= TOTAL_RECORDS_TO_PROCESS:
                print("Reached target number of records to process")
                break
        
        print(f"Processing completed. Total records processed: {total_processed}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
        
    finally:
        if source_session:
            source_session.close()
        if target_session:
            target_session.close()


if __name__ == "__main__":
    main()
