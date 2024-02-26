import hazelcast
import threading
import time


def produce(client_config, queue_name, start, end):
    client = hazelcast.HazelcastClient(**client_config)
    queue = client.get_queue(queue_name).blocking()
    for i in range(start, end + 1):
        queue.put(i)
        print(f"Produced: {i}")
    client.shutdown()


def consume(client_config, queue_name):
    client = hazelcast.HazelcastClient(**client_config)
    queue = client.get_queue(queue_name).blocking()
    while True:
        item = queue.take()
        print(f"{threading.current_thread().name} consumed: {item}")
    # client.shutdown()


def main():
    client_config = {
        "cluster_name": "dev",
    }
    queue_name = "my-bounded-queue"

    # Start producer
    producer_thread = threading.Thread(target=produce, args=(client_config, queue_name, 1, 100))
    producer_thread.start()

    # Start consumers
    consumer_threads = [
        threading.Thread(target=consume, args=(client_config, queue_name), name=f"Consumer-{i + 1}")
        for i in range(2)
    ]
    for consumer_thread in consumer_threads:
        consumer_thread.start()

    producer_thread.join()

    time.sleep(5)
    for consumer_thread in consumer_threads:

        consumer_thread.join()


if __name__ == "__main__":
    main()
