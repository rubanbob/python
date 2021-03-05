import jaydebeapi
import glob

# Dependencies
# pip install JayDeBeApi


jar_path = (glob.glob('/opt/project/jars/*.jar')) # list all jar's in the dir

def main():
    try:
        # create JDBC connection
        conn = jaydebeapi.connect("org.dbKind.jdbcDriver",
                                "jdbc:<dbType>://<host>:<port>/<db-name>",
                                ["username", "password"],
                                jar_path,)
        # init Cursor
        curs = conn.cursor()
        try:
            curs.execute("select * from TABLENAME")
            results = curs.fetchall()
            for result in results:
                print(result)
            curs.close()
            conn.close()
        except Exception as e:
            print("SQL query execution failed : ",e)

    except Exception as e:
        print("DB connection failed : ",e)

if __name__ == "__main__":
    print("Starting JDBC conneciton..")
    main()
