import psycopg2
from tiquu_pg.config import load_config

def insert_record(input_op,**kwargs):
    iud_operation=input_op
    if iud_operation == 'INSERT':
        print("insert the record")
        db_query = f"{iud_operation} INTO {kwargs['schema']} (journal_name, hash_value) VALUES ('{kwargs['journal_name']}', '{kwargs['hash_value']}')"
        print(db_query)
        """ Insert a new vendor into the vendors table """

        # sql = """INSERT INTO web_crawler_journals (journal_name, hash_value) VALUES ('gatesopen', 'ed8a9b1bed482be739183cab64b2a8a9c18b6d7b25ce2898cc0ffcb69cd4675b');"""
        
        # vendor_id = None
        config = load_config()

        try:
            with  psycopg2.connect(**config) as conn:
                with  conn.cursor() as cur_insert:
                    # execute the INSERT statement
                    # cur.execute(db_query, (vendor_name,))
                    cur_insert.execute(db_query)

                    # get the generated id back                
                    # rows = cur_insert.fetchone()
                    # print("rows -->",rows)
                    # if rows:
                    #     row_id = rows[0]
                    #     print(row_id)

                    # commit the changes to the database
                    conn.commit()
        # except (Exception, psycopg2.DatabaseError) as error:
        except Exception as error:
            print("Error:",error)    
        finally:
            return "success"
    

# if __name__ == '__main__':
#     insert_record(input_op,**kwargs)