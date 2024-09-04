# Note, you must have the keytab env variable set, e.g.:
# KRB5_CLIENT_KTNAME=/home/myuser/some_kerberos.keytab
from kafka import KafkaProducer
from datetime import datetime as dt
import ssl
import os

# Set the keytab environment variable
os.environ['KRB5_CLIENT_KTNAME'] = '/path/to/user.keytab'

broker = ['ca2u01hdku01d.quintiles.net:9093']

# Create an SSL context and disable certificate verification
ssl_context = ssl.create_default_context()
# if ssl failure occurs enable below lines
#ssl_context.check_hostname = False
#ssl_context.verify_mode = ssl.CERT_NONE

producer = KafkaProducer(bootstrap_servers=broker,
                         security_protocol='SASL_SSL',
                         sasl_mechanism='GSSAPI',
                         ssl_context=ssl_context)

msg = f'test-{dt.utcnow().isoformat()}'
future = producer.send(topic='topic_name', value=msg.encode('utf-8'))

# Check the response
try:
    record_metadata = future.get(timeout=10)
    print(f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
except Exception as e:
    print(f"Failed to send message: {e}")
