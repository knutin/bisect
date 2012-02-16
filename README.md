## Space-efficient dictionary

Recently at work I wrote a service to store some hundred million keys
in memory for fast lookups of multiple keys. The key is a 15 byte
string and the value is a binary where each bit is a flag.

Initially, I choose Redis for this task as it has the `MGET` command
which you can use to fetch multiple keys in a single roundtrip. As a
memory optimization, I can store the keys in a Redis hash and use a
pipelined request to fetch multiple keys. Redis stores 1 million keys
in 100 MB.

However, I was curious to see if I could improve on this setup. For
now the system will answer around 1000 concurrent requests, where each
request will look up 200 keys on average and 1000 keys in the worst
case. The cache will be updated a few hundred times per second. A
shared memory architecture would be the perfect tool for this task,
but I wanted to see how far I could push Erlang.

At first I tried using ETS, but some quick math showed that it would
not be feasible as the memory usage was too high (6 words + size of
data). Then I tried a trie implemented in pure Erlang, to spread out
the cost of storing the keys across all entries, however the memory
overhead of the Erlang tuples was too high to make even this approach
feasible.

In `src/bisect.erl` is an implementation of a key-value dictionary
using a large binary for storage. The key and value are fixed size
binaries so the binary behaves very much like an array. There is no
memory overhead per entry. Access is O(log n).

As large binaries are shared between processes, there can be multiple
concurrent readers, which fits the use case very well.

In `bisect:speed_test/0` is a micro-benchmark of sequential reads. By
adjusting `N` to a number that makes sense for your use case, you can
get an idea of the read performance.

