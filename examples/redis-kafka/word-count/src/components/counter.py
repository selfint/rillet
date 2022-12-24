import sys

from kafka import KafkaProducer, KafkaConsumer
from kafka.producer.future import FutureRecordMetadata
from kafka.consumer.fetcher import ConsumerRecord


BOOTSTRAP_SERVERS = sys.argv[1]
INPUT_TOPIC = "sentences"
OUTPUT_TOPIC = "sentences-word-count"
NUMBER_BITS = 64


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = KafkaConsumer(INPUT_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)

    for msg in consumer:
        msg: ConsumerRecord = msg
        word_count = len(msg.value.split()).to_bytes(NUMBER_BITS)

        future: FutureRecordMetadata = producer.send(
            topic=OUTPUT_TOPIC, value=word_count
        )

        future.get()
