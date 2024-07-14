import psycopg2
from tiquu_pg.config import load_config


def get_vendors():
    """ Retrieve data from the vendors table """
    config  = load_config()
    commands = [
        """
        SELECT schemaname,relname,n_live_tup 
        FROM pg_stat_user_tables 
        ORDER BY n_live_tup DESC;
        """,
        """
        select * from web_crawler_journals;
        """]
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(commands[0])
                print("The number of parts: ", cur.rowcount)
                row = cur.fetchone()

                while row is not None:
                    print(row)
                    row = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    get_vendors() 