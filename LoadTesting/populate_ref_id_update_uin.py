import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List
import time

CONFIG = {
    'SOURCE_DB': {  # MOSIP IDGen DB Session
        'host': 'localhost',
        'port': 5433,
        'user': 'postgres',
        'password': 'password',
        'database': 'mosip_kernel'
    },
    'TARGET_DB': { # Social Registry DB Session
        'host': 'localhost',
        'port': 5432,
        'user': 'socialregistryuser',
        'password': 'password',
        'database': 'socialregistrydb'
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


# GET UNUSED UINs FROM kernel.uin
def get_unused_uins(session: psycopg2.extensions.connection, BATCH_SIZE: int) -> List[str]:
    with session.cursor() as cur:
        cur.execute("""
            SELECT uin 
            FROM kernel.uin
            WHERE uin_status = 'UNUSED'
            LIMIT %s
        """, (BATCH_SIZE,))

        return [row[0] for row in cur.fetchall()]


# GET res_partner IDs WHERE ref_id IS NULL
def get_res_partners_without_ref_id(session: psycopg2.extensions.connection, BATCH_SIZE: int) -> List[int]:
    with session.cursor() as cur:
        cur.execute("""
            SELECT id
            FROM res_partner
            WHERE ref_id IS NULL
            LIMIT %s
        """, (BATCH_SIZE,))
        return [row[0] for row in cur.fetchall()]


# UPDATE ref_id IN res_partner
def update_ref_ids(session: psycopg2.extensions.connection, res_partner_ids: List[int], uins: List[str]) -> bool:
    try:
        with session.cursor() as cur:
            # Build the list of tuples (id, uin)
            update_values = list(zip(res_partner_ids, uins))
            execute_values(cur, """
                UPDATE res_partner rp
                SET ref_id = data.uin
                FROM (VALUES %s) AS data(id, uin)
                WHERE rp.id = data.id
            """, update_values)
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        print(f"Failed to update ref_ids: {e}")
        return False


# UPDATE uin_status = ISSUED IN kernel.uin
def update_uin_status(session: psycopg2.extensions.connection, uins: List[str]) -> bool:
    try:
        with session.cursor() as cur:
            cur.execute("""
                UPDATE kernel.uin 
                SET uin_status = 'ASSIGNED'
                WHERE uin = ANY(%s)
            """, (uins,))
        session.commit()

        return True

    except Exception as e:
        session.rollback()
        print(f"Failed to update UIN status: {e}")
        return False


def main():
    source_session = None
    target_session = None

    TOTAL_RECORDS_TO_PROCESS = 40000
    BATCH_SIZE = 40000

    try:
        source_session = get_db_session(CONFIG['SOURCE_DB']) # MOSIP IDGen DB Session
        target_session = get_db_session(CONFIG['TARGET_DB']) # Social Registry DB Session

        total_processed = 0
        start_time = time.time()

        while True:
            # Calculate current batch size
            remaining_records = TOTAL_RECORDS_TO_PROCESS - total_processed
            current_batch_size = min(BATCH_SIZE, remaining_records)

            # Fetch res_partner records with ref_id is null
            res_partner_ids = get_res_partners_without_ref_id(target_session, current_batch_size)

            # No more records to process
            if not res_partner_ids:
                print("No more res_partner records without ref_id")
                break

            # Get UINs from source database
            uins = get_unused_uins(source_session, len(res_partner_ids))

            # No more UINs to assign
            if not uins:
                print("No more UINs available")
                break

            actual_batch_size = min(len(res_partner_ids), len(uins))
            print(f"Processing batch of {actual_batch_size} records")

            # Truncate lists to the actual batch size
            res_partner_ids = res_partner_ids[:actual_batch_size]
            uins = uins[:actual_batch_size]

            # Update ref_ids in res_partner
            if update_ref_ids(target_session, res_partner_ids, uins):
                # Update uin_status in source database
                if update_uin_status(source_session, uins):
                    total_processed += actual_batch_size
                    print(f"Successfully processed {actual_batch_size} records")
                else:
                    print("Failed to update uin_status, rolling back batch")
                    continue
            else:
                print("Failed to update ref_ids, skipping batch")
                continue

            # Elapsed and rate statistics
            elapsed_time = time.time() - start_time
            rate = total_processed / elapsed_time if elapsed_time > 0 else 0
            print(
                f"Progress: {total_processed}/{TOTAL_RECORDS_TO_PROCESS} records processed ({rate:.2f} records/second)")

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
