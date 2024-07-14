import psycopg2
from tiquu_pg.config import load_config



# def db_query(iud_op,**kwargs):
#     iud_operation = iud_op

#     if iud_operation == 'SELECT':
#         db_query = f"{iud_operation} count(*) from {kwargs['schema']} where journal_name = '{kwargs['journal_name']}' AND hash_value = '{kwargs['hash_value']}'"
#         print(db_query)
#         try:
#             # execute_query(db_query,{'journal_name':kwargs['journal_name'],'hash_value':kwargs['hash_value']})
#             conn = sqlite3.connect('tiquu.db')
#             cursor = conn.cursor()
#             cursor.execute(db_query)
#             result = cursor.fetchone()
#             cursor.close()
#             conn.close()
#             print(result)
#             return result
#         except:
#             print('select query failed')
#     elif iud_operation == 'INSERT':
#         print("insert the record")
#         db_query = f"{iud_operation} INTO {kwargs['schema']} (journal_name, hash_value) VALUES ('{kwargs['journal_name']}', '{kwargs['hash_value']}')"
#         print(db_query)
#         try:
#             # execute_query(db_query,{'journal_name':kwargs['journal_name'],'hash_value':kwargs['hash_value']})
#             conn = sqlite3.connect('tiquu.db')
#             cursor = conn.cursor()
#             cursor.execute(db_query)
#             conn.commit()
#             result = cursor.fetchone()
#             cursor.close()
#             conn.close()
#             print(result)
#             return result
#         except:
#             print('insert query failed')

def get_records(iud_op,**kwargs):
    """ Retrieve data from the vendors table """
    config  = load_config()
    iud_operation = iud_op
    try:
        if iud_operation =='SELECT':
            db_query = f"{iud_operation} count(*) from {kwargs['schema']} where journal_name = '{kwargs['journal_name']}' AND hash_value = '{kwargs['hash_value']}'"
            print(db_query)
            with psycopg2.connect(**config) as conn:
                with conn.cursor() as cur:
                    cur.execute(db_query)
                    print("The number of rows: ", cur.rowcount)
                    # return cur.rowcount
                    row = cur.fetchone()
                    return row[0]
                    # if row[0]==0: return row

                    # while row is not None:
                    #     print(row,type(row))
                    #     row = cur.fetchone()
                    #     return row
                        

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    get_records()        