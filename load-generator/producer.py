import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            max_block_ms=10000,  # Fail if unable to send after 10 seconds
            retries=0  # Do not retry indefinitely
        )
        topic = 'my-topic'
        message_count = 0

        while True:
            message = f'Message {message_count}'.encode('utf-8')
            try:
                producer.send(topic, value=message)
                print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Sent: {message}')
            except KafkaError as e:
                print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Failed to send message: {e}')
            message_count += 1
            time.sleep(0.1)  # Ensure this is inside the loop
    except NoBrokersAvailable:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} No brokers available.')
    except Exception as e:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Error in producer: {e}')

if __name__ == '__main__':
    main()