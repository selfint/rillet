import sys
import time

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = sys.argv[1]
INPUT_TOPIC = "word-count"
PRINT_RATE_SEC = 3


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = KafkaConsumer(INPUT_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)

    msgs = 0
    start = time.time()
    print_rate_ms = PRINT_RATE_SEC * 1000

    for msg in consumer:
        msgs += 1

        current_time = time.time()
        if current_time >= start + PRINT_RATE_SEC:
            print(f"Rate={msgs/(print_rate_ms):.3} msgs/ms")
            sys.stdout.flush()
            msgs = 0
            start = current_time
