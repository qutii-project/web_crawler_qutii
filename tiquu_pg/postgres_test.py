import psycopg2


# conn = psycopg2.connect(
#                         host="http://library.clfojyqicnb4.eu-west-2.rds.amazonaws.com/",
#                         user="dev_user",
#                         password="db_pass",
#                         port="5432")

engine = psycopg2.connect(
    database="postgres",
    user="dev_user",
    password="password",
    host="library.clfojyqicnb4.eu-west-2.rds.amazonaws.com",
    port='5432'
)