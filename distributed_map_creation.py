import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(
        cluster_name="dev",
    )

    # Create a Distributed Map in the cluster
    my_map = client.get_map("my-distributed-map").blocking()

    for i in range(1000):
        my_map.put(i, f"value-{i}")