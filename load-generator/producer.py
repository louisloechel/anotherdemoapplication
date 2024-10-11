import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError # type: ignore
import json


def main():
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
            message_count = 0

            while True:
                message = json.dumps({"message_count": message_count}).encode('utf-8')
                try:
                    if message_count % 2 == 0:
                        producer.send(topic, value=message)
                        print(f'Sent: {message} to Topic: {topic}', flush=True)
                    else:
                        producer2.send(topic2, value=message)
                        print(f'Sent: {message} to Topic: {topic2}', flush=True)
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
            break
    else:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Failed to connect to Kafka broker after multiple attempts.', flush=True)


if __name__ == '__main__':
    main()