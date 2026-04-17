from utils import get_db_connection
import os
from dotenv import load_dotenv

load_dotenv()

def check_data_count():
    conn = get_db_connection()
    if conn is None:
        print("Could not connect to database.")
        return

    table_name = os.getenv('TABLE_NAME', 'rossman_deployed')
    try:
        with conn.cursor() as cur:
            # Get total count
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            total_count = cur.fetchone()[0]
            
            # Get breakdown by data_source
            cur.execute(f"SELECT data_source, COUNT(*) FROM {table_name} GROUP BY data_source;")
            breakdown = cur.fetchall()
            
            print(f"Total entries in '{table_name}': {total_count}")
            if breakdown:
                print("\nBreakdown by data_source:")
                for source, count in breakdown:
                    print(f"- {source}: {count}")
    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_data_count()
