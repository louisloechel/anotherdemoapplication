import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError # type: ignore
import json
import traceback

def main():
    # load news2 data from data.json
    try:
        with open('data.json', 'r') as f:
            data = json.load(f)
        message_count = 0
    except FileNotFoundError:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} data.json file not found.', flush=True)
        return
    except json.JSONDecodeError as e:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Error decoding JSON: {e}', flush=True)
        return
    except Exception as e:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Unexpected error: {e}', flush=True)
        return

        
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:29092',
                max_block_ms=10000,  # Fail if unable to send after 10 seconds
                retries=0  # Do not retry indefinitely
            )
            topic = 'raw-topic'

            producer2 = KafkaProducer(
                bootstrap_servers='kafka:29092',
                max_block_ms=10000,  # Fail if unable to send after 10 seconds
                retries=0  # Do not retry indefinitely
            )
            topic2 = 'processed-topic'
            producer2.send(topic2, value=b'{}')
            
            while True:
                print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Sending messages...', flush=True)
                if message_count >= len(data):
                    message_count = 0
                # message = #message_count entry in data.json
                message = data[message_count]
                loaded_message = json.dumps(message).encode('utf-8')
                print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Loaded message: {loaded_message}', flush=True)
                try:
                    producer.send(topic, value=loaded_message)
                    print(f'Sent: {loaded_message} to Topic: {topic}', flush=True)
                except KafkaError as e:
                    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Failed to send message: {e}', flush=True)
                message_count += 1
                time.sleep(0.1)
        except NoBrokersAvailable:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} No brokers available. Retrying...', flush=True)
            retries -= 1
            time.sleep(5)  # Wait before retrying
        except KafkaTimeoutError as e:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Kafka timeout error: {e}. Retrying...', flush=True)
            retries -= 1
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Error in producer: {e}', flush=True)
            print(traceback.format_exc(), flush=True)
            break
    else:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Failed to connect to Kafka broker after multiple attempts.', flush=True)


if __name__ == '__main__':
    main()