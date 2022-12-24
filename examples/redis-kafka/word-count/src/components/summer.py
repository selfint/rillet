import sys

from kafka import KafkaProducer, KafkaConsumer
from kafka.producer.future import FutureRecordMetadata
from kafka.consumer.fetcher import ConsumerRecord
from redis import Redis


BOOTSTRAP_SERVERS = sys.argv[1]
REDIS_SERVER = sys.argv[2]
INPUT_TOPIC = "sentences-word-count"
OUTPUT_TOPIC = "word-count"
REDIS_KEY = "summer-state"
NUMBER_BITS = 64


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer = KafkaConsumer(INPUT_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)

    redis_host, redis_port = REDIS_SERVER.split(":")
    redis = Redis(host=redis_host, port=redis_port, db=0)

    total_word_count: int = int(redis.get(REDIS_KEY) or 0)

    for msg in consumer:
        msg: ConsumerRecord = msg
        word_count = int.from_bytes(msg.value)

        total_word_count += word_count

        total_word_count_bytes = total_word_count.to_bytes(NUMBER_BITS)
        future: FutureRecordMetadata = producer.send(
            topic=OUTPUT_TOPIC, value=total_word_count_bytes
        )

        redis.set(REDIS_KEY, total_word_count)

        future.get()
