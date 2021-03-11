import jaydebeapi
import glob

jar_path = (glob.glob('./jars/*.jar')) # list all jar's in the dir
print("available jar in path :",jar_path)

def main(jdbc_driver, jdbc_url, jdbc_uname, jdbc_pass, query):
    try:
        # create JDBC connection
        conn = jaydebeapi.connect(jdbc_driver,
                                jdbc_url,
                                [jdbc_uname, jdbc_pass],
                                jar_path,)
        # init Cursor
        curs = conn.cursor()
        try:
            curs.execute(query)
            #rows = curs.fetchall()
            while True:
                # fetch in batch
                rows = curs.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    print(row)
            curs.close()
            conn.close()
        except Exception as e:
            conn.close()
            print("SQL query execution failed : ",e)

    except Exception as e:
        print("DB connection failed : ",e)

if __name__ == "__main__":
    print("Starting JDBC conneciton..")

    # inputs
    jdbc_driver = "org.postgresql.Driver"
    jdbc_url = "jdbc:postgres://<host>:5432/<db-name>"
    jdbc_uname = "postgres"
    jdbc_pass = "postgres-password"

    query =  "select * from TABLENAME"

    main(jdbc_driver, jdbc_url, jdbc_uname, jdbc_pass, query)
