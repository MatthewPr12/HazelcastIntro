import hazelcast
import threading


def increment_map_value(client_config, key, iterations):
    client = hazelcast.HazelcastClient(**client_config)
    my_map = client.get_map("my-distributed-map").blocking()
    my_map.put_if_absent(key, 0)
    for _ in range(iterations):
        current_value = my_map.get(key)
        my_map.put(key, current_value + 1)
    client.shutdown()


def main():
    client_config = {
        "cluster_name": "dev",
    }
    key = "key"
    iterations = 10_000
    threads = [threading.Thread(target=increment_map_value, args=(client_config, key, iterations)) for _ in range(3)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # To check the final value, create a temporary client
    temp_client = hazelcast.HazelcastClient(**client_config)
    final_value = temp_client.get_map("my-distributed-map").blocking().get(key)
    print(f"Final value without locks: {final_value}")
    temp_client.shutdown()


if __name__ == "__main__":
    main()
