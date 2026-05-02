import datetime
import logging
import random
import threading
import uuid

from authlib.common.encoding import json_dumps
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "financial_transactions"
logging.basicConfig(level=logging.INFO)

loggers = logging.getLogger(__name__)

producer_config = {
    "bootstrap.servers": KAFKA_BROKERS,
    "queue.buffering.max.ms": 10000,
    "queue.buffering.max.kbytes": 512000,
    "batch.num.messages": 1000,
    "linger.ms":10,
    "acks": 1,
    "compression.type": "gzip"
}

producer = Producer(producer_config)

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )

            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    #print(f"Created successfully topic {topic_name} in partition {NUM_PARTITIONS} with replication_factor {REPLICATION_FACTOR} and partition {NUM_PARTITIONS}")
                    logging.info(f"Created successfully topic {topic_name} in partition {NUM_PARTITIONS} "
                                 f"with replication_factor {REPLICATION_FACTOR} and partition {NUM_PARTITIONS}")
                except Exception as e:
                    #print(f"Failed to create topic {topic_name} with error {e}")
                    logging.error(f"Failed to create topic {topic_name} with error {e}")
        else:
            #print(f"Topic {topic_name} already exists")
            logging.info(f"Topic {topic_name} already exists")
    except Exception as e:
        #print(f"Failed to create topic {topic_name} with error {e}")
        logging.error(f"Failed to create topic {topic_name} with error {e}")

def generate_transactions():
    return dict(
        transactioId = str(uuid.uuid4()),
        userId = f"user_{random.randint(1, 100)}",
        amount = round(random.uniform(50000, 150000),2),
        transactionTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        merchantId = random.choice(['merchant1', 'merchant2','merchant3']),
        transactionType = random.choice(['purchase', 'refund']),
        location = f'location {random.randint(1, 50)}',
        paymentMethod = random.choice(['credit-card','debit-card','pix']),
        isInternational = random.choice([True, False]),
        curreency = random.choice(['USD','EUR','BRL'])
    )

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed for record: {msg.key()}')
    else:
        print(f'Record: {msg.key()} successfully produced')

def produce_transaction(thread_id):
    while True:
        transactions = generate_transactions()

        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transactions["userId"],
                value=json_dumps(transactions).encode("utf-8"),
                on_delivery=delivery_report
            )
            print(f"Produced transactions for user "
                  f"{transactions['userId']}")
            print(f"Produced transaction:{transactions}")
            producer.flush()
        except Exception as e:
            print(f"Error sending transactions: {e}")

def producer_data_in_parallel(num_threads):
    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transaction, args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except Exception as e:
        print(f"Error sending transactions: {e}")

if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    producer_data_in_parallel(3)
    # while True:
    #     transactions = generate_transactions()
    #
    #     try:
    #         producer.produce(
    #             topic=TOPIC_NAME,
    #             key=transactions["userId"],
    #             value=json_dumps(transactions).encode("utf-8"),
    #             on_delivery=delivery_report
    #         )
    #         print(f"Produced transactions for user "
    #               f"{transactions['userId']}")
    #         print(f"Produced transaction:{transactions}")
    #         producer.flush()
    #     except Exception as e:
    #         print(f"Error sending transactions: {e}")
