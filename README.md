# Gossip Glomers Challenge

## Challenge #2: Unique ID Generation

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-2-unique-id/main.go)

My unique ID is the concatenation of the current nano timestamp and a cryptographically secure random integer (using `math/rand` wasn't passing the test).

## Challenge #3: Broadcast

### #3a: Single-Node Broadcast

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3a-broadcast/main.go)

### #3b: Multi-Node Broadcast

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3b-broadcast/main.go)

In this solution, I use something other than the suggested topology. As we don't have any constraints, every node broadcasts to all the nodes using `Send`. It could be way more efficient, but it works. In [#3d](#3d--efficient-broadcast-part-i), the solution will be smarter.

### #3c: Fault Tolerant Broadcast

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3c-broadcast/main.go)

Same code as [#3b](#3b--multi-node-broadcast). It turns out that a dumb solution to broadcast to all the nodes also solves network partition issues 😅. Same as before, in [#3d](#3d--efficient-broadcast-part-i), the solution will contain some proper retry.

### #3d: Efficient Broadcast, Part I

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3d-broadcast/main.go)

Things are starting to get challenging. We have some constraints in the number of messages exchanged (goodbye good old dumb solution that broadcasts to all the nodes) and the latency.

Regarding the latency, the maximum should be 600ms. As exchanging a message takes 100ms, we need to have a topology where the distance between any node never exceeds 5.

The topology that I used is a _flat_ tree. One root node and the rest are children nodes; hence, a tree with only two levels:

![](res/tree.png)

In this topology, a child node broadcasts a message only to the root node and the root node to all the children. Hence, the maximum distance between any nodes is 2 (e.g., 24 -> 0 -> 3).

In the end, the solution achieves the following results:
* Messages-per-operation: 23.38 (it's the result returned by maelstrom, I guess the average value is below 24 because it only counts the messages-per-operation for the other operations, such as `read` 🤷)
* Median latency: 181ms
* Maximum latency: 207ms

I also switched from `Send` to `SyncRPC` to detect network partitions and implement a proper retry mechanism.

The main downside of this solution is that the broadcast load isn't evenly spread among the nodes. Indeed, the root node becomes a hot spot. In case of this node becomes inaccessible, it means that until it's fixed, none of the nodes will receive any broadcast message. That's the main tradeoff with this solution.

### #3e: Efficient Broadcast, Part II

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3e-broadcast/main.go)

Now we need to make the messages-per-operation below 20 (below the number of nodes). I used a batch operation where each node now broadcasts such a message:

```json
{
  "type": "broadcast",
  "messages": [1, 8, 72, 25]
}
```

So, instead of sending a single message value, we can now broadcast multiple messages at once. I do it via a goroutine scheduled every 500ms, which checks what has to be broadcasted and then sends the messages.

* Messages-per-operation: 1.92
* Median latency: 764ms
* Maximum latency: 1087ms

It's also interesting to play with the frequency. Increasing the value means decreasing messages-per-operation but increasing the latencies. No solution is perfect; everything is a question of tradeoffs and what's the best balance given a specific context.

The solution also handles network partitions.

## Challenge #4: Grow-Only Counter

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-4-grow-only-counter/main.go)

We need to work with a sequentially-consistent store. The main impact is that when doing a read, the only client that will have the guarantee to get the latest value is the one who performed the write.

In this solution, each node writes to its own bucket. Then, the solution relies on some form of coordination among the nodes before returning a read request:
* Iterate over all the nodes of the cluster:
  * If the node is the current node, we perform a read in the store directly using `ReadInt`
  * Otherwise, it contacts the other node via a `SyncRPC` call to ask it to return the latest value (and this node will perform the read in the store using `ReadInt` as well)
* We return the sum of all the values

In the meantime, and even if it wasn't mandatory to pass all the tests (including the network partitions test), I introduced some forms of caching so if a node can't contact the store or another node, it will return the latest known value (availability > consistency). But again, it's just a question of tradeoff; if we remove the cache and return an error in case a node or the store is unreachable, we would favor consistency over availability.

## Challenge #5: Kafka-Style Log

### #5a: Single-Node Kafka-Style Log

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-5a-kafka-log/main.go)

Not much to say here. We store everything in memory with a map per `key`. As the logs are ordered, we can find the proper offset when replying to a `request` using a binary search.

### #5b: Multi-Node Kafka-Style Log

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-5b-kafka-log/main.go)

In this first distributed implementation, I decided to use three bucket types (by bucket, I mean entries in the store):
* One to store the latest offset for a given key
* One to store the latest committed offset for a given key
* And the last bucket to store the messages

For the latter, I chose to store a single entry per message. The main message is that I don't have to rely on `CompareAndSwap` to store messages. I only use it to store the latest offset. During the tests, it triggers a CAS retry about 40 times. Yet, the main downside is that the `poll` has a linear time complexity: we start from the provided offset, and then iterate until we reach an `KeyDoesNotExist` error. 

This solution leads to the following results: 

* Messages-per-operation: 12.03
* Availability: 0.9991992
* Throughput peak: ~300hz

### #5c: Efficient Kafka-Style Log

Here, we are asked to improve the overall latency. The solution I took is to switch from one entry per message to one entry per key (hence, one entry contains multiple messages). To do that while passing the test, my solution routes the message for a given key to the same instance using a hashing mechanism. That allows me to get rid of expensive CAS fail-and-retry operations. All the writes are done using `Write` using a shared mutex to protect concurrent writes that would result (if no mutex) in consistency issues.

Metrics-wise:

* Messages-per-operation: 6.88
* Availability: 0.99511856
* Throughput peak: ~350hz

So a small drop regarding availability, which can probably be explained by the fact that a `send` request needs to be forwarded synchronously to another node if the hashing doesn't match the node ID. Yet, it increases the messages-per-operation and the throughput.

One remark, though. In Kafka, this routing to the same node isn't achieved at the topic level. Imagine that a 3-node Kafka cluster has only one topic; we don't want to have only one node being a hot spot. Hence, Kafka introduces the concept of a partition, basically a sub-split per topic. If I wanted to improve my solution, I should probably do the same.

Also, another downside is that now I have only one entry for all the messages that belong to a specific key. If we receive too many messages, at some point, it will become an issue. To tackle that, and if I recall correctly, Kafka introduces the concept of segments, basically splitting the partitions into chunks (and a rotation either based on time or on size). Again, if I wanted to improve my solution, I should probably do it as well.
