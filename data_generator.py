import random, json
import datetime, time
import boto3

def getReferrer():
    x = random.randint(1,5)
    x = x*50 
    y = x+30 
    data = {}
    data['user_id'] = random.randint(x,y)
    data['device_id'] = random.choice(['mobile','computer', 'tablet', 'mobile','computer'])
    data['client_event'] = random.choice(['beer_vitrine_nav','beer_checkout','beer_product_detail',
    'beer_products','beer_selection','beer_cart'])
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['client_timestamp'] = str_now
    return data

kinesis_client = boto3.Session(
    aws_access_key_id="",
    aws_secret_access_key="",
).client("kinesis")
kinesis_data_stream = "clickstream001"

for i in range(6000000):
    data = json.dumps(getReferrer())
    print(data)
    kinesis_client.put_record(
    StreamName=kinesis_data_stream,
    Data=data,
    PartitionKey=str(i),
    )
    if(i%10 == 0):
        #print(data) 
        time.sleep(3)