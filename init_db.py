from utils import get_db_connection
from dotenv import load_dotenv
import os

load_dotenv()

def init_db():
    conn = get_db_connection()
    if conn is None:
        print("Could not connect to database. Make sure it's running and credentials in .env are correct.")
        return

    try:
        with conn.cursor() as cur:
            # Create the table if it doesn't exist
            table_name = os.getenv('TABLE_NAME', 'rossman_deployed')
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                store_id INTEGER NOT NULL,
                date DATE NOT NULL,
                promo INTEGER NOT NULL,
                state_holiday VARCHAR(5) NOT NULL,
                school_holiday INTEGER NOT NULL,
                prediction DOUBLE PRECISION NOT NULL,
                data_source VARCHAR(20) DEFAULT 'user'
            );
            """
            cur.execute(create_table_query)
            
            # Check if data_source column exists, if not, add it (for existing tables)
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='{table_name}' AND column_name='data_source';
            """)
            if not cur.fetchone():
                print(f"Adding 'data_source' column to {table_name}...")
                cur.execute(f"ALTER TABLE {table_name} ADD COLUMN data_source VARCHAR(20) DEFAULT 'user';")
            
            conn.commit()
            print(f"Database table '{table_name}' is ready.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
