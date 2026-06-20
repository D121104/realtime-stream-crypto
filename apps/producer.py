import uuid
import json
import websocket
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

TOPIC_NAME = 'crypto-raw-data'
SOCKET_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade"

def on_open(ws):
    print("connection established")

def on_message(ws, message):
    data_json = json.loads(message)
    
    stream_name = data_json.get('stream')
    
    payload = data_json.get('data')
    
    kafka_key = payload['s']

    metadata = {
        'event_id': str(uuid.uuid4()),
        'event_timestamp': payload['E'],
        'event_type': stream_name.split('@')[1],
        'source': 'binance_websocket',
        'schema_version': '1.0'
    }

    kafka_value = {
        'metadata': metadata,
        'payload': payload
    }



    try:
        producer.send(
            topic = TOPIC_NAME,
            key=kafka_key,
            value=kafka_value,
        )

        print(f" Pushed Structure Data | ID: {metadata['event_id']} | Key: {kafka_key}")
    except Exception as e:
        print(f"Error sending message to Kafka: {str(e)}")

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("connection closed")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(SOCKET_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()