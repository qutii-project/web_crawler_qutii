import psycopg2
from config import load_config

def create_tables():
    """ Create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE web_crawler_journals (
            id SERIAL PRIMARY KEY,
            journal_name TEXT NOT NULL,
            hash_value TEXT NOT NULL
                    );

        """,
        """
        CREATE TABLE web_crawler_pg_sequence (
            name TEXT,
            seq BIGINT
        );
        """
        )
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    cur.execute(command)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    create_tables()