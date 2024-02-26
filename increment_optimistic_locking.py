import hazelcast
import threading


def optimistic_increment(client_config, key, iterations):
    client = hazelcast.HazelcastClient(**client_config)
    my_map = client.get_map("my-distributed-map").blocking()

    my_map.put_if_absent(key, 0)

    for _ in range(iterations):
        success = False
        while not success:
            current_value = my_map.get(key)
            new_value = current_value + 1
            success = my_map.replace_if_same(key, current_value, new_value)

    client.shutdown()


def main():
    client_config = {
        "cluster_name": "dev",
    }
    key = "key_optimistic_locking3"
    iterations = 10_000
    threads = [
        threading.Thread(target=optimistic_increment, args=(client_config, key, iterations)) for _ in range(3)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    temp_client = hazelcast.HazelcastClient(**client_config)
    final_value = temp_client.get_map("my-distributed-map").blocking().get(key)
    print(f"Global Final value with optimistic locking: {final_value}")
    temp_client.shutdown()


if __name__ == "__main__":
    main()
