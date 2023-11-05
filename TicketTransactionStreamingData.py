import boto3
import random
import datetime
import time
import json
import ipaddress

# 1. Client Kinesis Data Streaming
kinesis_client = boto3.client('kinesis',
                                aws_access_key_id='YOUR_ACCESS_KEY_ID',
                                aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                                region_name='YOUR_REGION') 

# 2. Function for generate a Ticket Transaction Streaming Data
def generar_transaccion_datos():
  transaction = {
    "customerId": random.randint(1, 50),
    "transactionAmount": random.randint(10, 150),
    "sourceIp": str(ipaddress.IPv4Address(random.randint(0, 2**32 - 1))),
    "status": random.choices(["OK", "FAIL", "PENDING"], [0.8, 0.1, 0.1])[0],
    "transactionTime": datetime.datetime.now().isoformat()
  }

  return transaction

while True:

  data_streaming = json.dumps(generar_transaccion_datos())+ '\n'
  time.sleep(1)
  print(data_streaming)

  # 3. Send data to Kinesis Data Firehose
  response = kinesis_client.put_record(
  StreamName='TicketTransactionStreamingData',  # Reemplaza con el nombre de tu stream
  Data=data_streaming, # Los datos en formato JSON o cadena de texto
  PartitionKey='2'
  )