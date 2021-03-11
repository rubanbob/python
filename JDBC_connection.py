import jaydebeapi
import glob

jar_path = (glob.glob('./jars/*.jar')) # list all jar's in the dir
print("available jar in path :",jar_path)

def main():
    try:
        # create JDBC connection
        conn = jaydebeapi.connect("org.postgresql.Driver",
                                "jdbc:postgres://<host>:5432/<db-name>",
                                ["postgres", "postgres-password"],
                                jar_path,)
        # init Cursor
        curs = conn.cursor()
        try:
            curs.execute("select * from TABLENAME")
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
    main()
