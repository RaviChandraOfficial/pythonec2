

import json
import psycopg2
import paho.mqtt.client as mqtt

# RDS PostgreSQL connection parameters
host = "database.cfsg8aggwxm9.us-east-1.rds.amazonaws.com"
username = "postgres"
password = "Sbikrc21916"
database = "postgres"

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("test1234")

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

    message = msg.payload.decode()  # Decode bytes to string
    print(f"Received message: {message}")

    iot_message = json.loads(msg.payload)
    print(type(iot_message))
    
    sensor_id = iot_message.get('sensor_id')
    value = iot_message.get('value')
    
    if sensor_id is None or value is None:
        print("Missing required fields in the IoT message.")
        return
    
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=username,
            password=password
        )
        cur = conn.cursor()
        
        insert_query = """
        INSERT INTO sensor (sensor_id, value) 
        VALUES (%s, %s)
        """
        cur.execute(insert_query, (sensor_id, value))

        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Set the path to your CA certificate, client certificate, and private key
ca_path = "AmazonRootCA1.pem"
cert_path = "4fcec45f0562bdfe59fd9b02e6930d2bc7258358d968dd28afcb4044dd8a1c3d-certificate.pem.crt"
key_path = "4fcec45f0562bdfe59fd9b02e6930d2bc7258358d968dd28afcb4044dd8a1c3d-private.pem.key"

client.tls_set(ca_certs=ca_path, certfile=cert_path, keyfile=key_path)

# Replace 'your-iot-endpoint.amazonaws.com' with your actual AWS IoT endpoint
client.connect("a1zpb2m9wn3lmq-ats.iot.us-east-1.amazonaws.com", 8883, 60)
client.loop_forever()
