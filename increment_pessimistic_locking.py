import hazelcast
import threading


def increment_map_value_with_lock(client_config, key, iterations):
    client = hazelcast.HazelcastClient(**client_config)
    my_map = client.get_map("my-distributed-map").blocking()
    my_map.put_if_absent(key, 0)
    # lock = client.get_cp_subsystem().get_lock("my-lock")
    for _ in range(iterations):
        my_map.lock(key)
        try:
            current_value = my_map.get(key)
            my_map.put(key, current_value + 1)
        finally:
            my_map.unlock(key)
    client.shutdown()


def main():
    client_config = {
        "cluster_name": "dev",
    }
    key = "key_pessimistic_locking"
    iterations = 10_000
    threads = [threading.Thread(target=increment_map_value_with_lock, args=(client_config, key, iterations)) for _ in
               range(3)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    temp_client = hazelcast.HazelcastClient(**client_config)
    final_value = temp_client.get_map("my-distributed-map").blocking().get(key)
    print(f"Final value with pessimistic locking: {final_value}")
    temp_client.shutdown()


if __name__ == "__main__":
    main()
