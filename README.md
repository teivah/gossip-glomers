# Gossip Glomers Challenge

## Challenge #2: Unique ID Generation

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-2-unique-id/main.go)

The unique ID is the concatenation of the current nano timestamp and a cryptographically secure random integer (using `math/rand` wasn't passing the test).

## Challenge #3: Broadcast

### #3a: Single-Node Broadcast

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3a-broadcast/main.go)

### #3b: Multi-Node Broadcast

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3b-broadcast/main.go)

In this solution, I use something other than the suggested topology. As we don't have any constraints, every node broadcasts to all the nodes using `Send`. It could be way more efficient, but it works. In [#3d](#3d--efficient-broadcast-part-i), the solution will be smarter.

### #3c: Fault Tolerant Broadcast

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3c-broadcast/main.go)

Same code as [#3b](#3b--multi-node-broadcast). It turns out that a dumb solution to broadcast to all the nodes also tackles solves network partition issues 😅. Same as before, in [#3d](#3d--efficient-broadcast-part-i), the solution contains some proper retry.

### #3d: Efficient Broadcast, Part I

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3d-broadcast/main.go)

Things are starting to get challenging. We have some constraints in the number of messages exchanged (goodbye good old dumb solution that broadcasts to all the nodes) and the latency.

Regarding the latency, the maximum should be 600ms. As exchanging a message takes 100ms, we need to have a topology where the distance between any node never exceeds 5.

The topology that I used is a _flat_ tree. One root node, the rest are children nodes; hence, a tree with only two levels:

![](res/tree.png)

In this topology, a child node broadcasts a message only with the root node and the root node to all the children. Hence, the maximum distance between any nodes is 2 (e.g., 24 -> 0 -> 3).

In the end, my solution achieves the following:
* Messages-per-operation: 23.38 (it's the result returned by maelstrom, I guess the average value is below 24 because it only counts the messages-per-operation for the other operations, such as `read`?)
* Median latency: 181ms
* Maximum latency: 207ms

I also switch from `Send` to `SyncRPC` to detect network partitions and implement a proper retry mechanism.

The main downside of this solution is that the broadcast load isn't evenly spread among the nodes. Indeed, the root node becomes a hot spot. In case of this node becomes inaccessible, it means that until it's fixed, none of the nodes will receive any update.

### #3e: Efficient Broadcast, Part II

[Solution](https://github.com/teivah/gossip-glomers/blob/main/challenge-3e-broadcast/main.go)

Now we need to make the messages-per-operation below 20 (below the number of nodes). I used a batch operation where each node now broadcasts such a message:

```json
{
  "type": "broadcast",
  "messages": [1, 8, 72, 25]
}
```

So, instead of sending a single message value, we can now broadcast multiple messages at once. I do it via a goroutine scheduled every 500ms, check what has to be broadcasted, and then send the messages.

* Messages-per-operation: 1.92
* Median latency: 764ms
* Maximum latency: 1087ms

It's also interesting to play with the frequency. Increasing the value means decreasing messages-per-operation but increasing the latencies.

The solution also handles network partitions.
