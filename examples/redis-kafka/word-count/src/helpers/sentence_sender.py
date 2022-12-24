import sys
import time

from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata

BOOTSTRAP_SERVERS = sys.argv[1]
OUTPUT_TOPIC = "sentences"
SENTENCE = b"The quick brown fox jumps over the lazy dog"
PRINT_RATE_SEC = 3


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    msg_size = len(SENTENCE)

    bytes_sent = 0
    start = time.time()
    print_rate_ms = PRINT_RATE_SEC * 1000

    while True:
        future: FutureRecordMetadata = producer.send(topic=OUTPUT_TOPIC, value=SENTENCE)

        future.get()
        bytes_sent += msg_size

        current_time = time.time()
        if current_time >= start + PRINT_RATE_SEC:
            print(f"Rate={bytes_sent/(print_rate_ms):.3} bytes/ms")
            sys.stdout.flush()
            bytes_sent = 0
            start = current_time
