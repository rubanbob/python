#-*- coding: utf-8 -*-
import jaydebeapi
import jpype
import datetime

# required belo files
jaas = './config/jass.conf'
krb5 = './config/krb5.conf'
keytab = './config/user.keytab'
jar = './jars/HiveJDBC41.jar'
principal = "hive/host@domain"

try:
    print("establishing connection")

    jvm_path = jpype.getDefaultJVMPath()
    print(jvm_path)

    jpype.startJVM(jvm_path, 
                '-Djava.class.path='+ jar,
                '-Djavax.security.auth.useSubjectCredsOnly=true',
                '-Djava.security.krb5.conf='+krb5,
                '-Djava.security.auth.login.config='+jaas,
                '-Dhadoop.security.authentication="kerberos"', 
                )


    jdbc_url = "jdbc:hive2://host:port/default;KrbRealm=domain;AuthMech=1;KrbHostFQDN=host;KrbServiceName=hive;principal=hive/host@domain;ssl=1;"
    conn = jaydebeapi.connect("com.cloudera.hive.jdbc41.HS2Driver", jdbc_url)

    print("create cursor")
    curs = conn.cursor()
    print("unload start : ",datetime.datetime.now())\
    
    curs.execute("show databases")

    results = curs.fetchall()
    for row in results:
        print(row)

except Exception as err:
    print("!Error :",err)
finally:
    try: curs.close()
    except Exception as err: pass
    try: conn.close()
    except Exception as err: pass

print("unload end : ",datetime.datetime.now())

