# Bisect

Bisect is a dictionary-like data structure with some very nice properties:

 * Fixed-size key and values, no storage overhead
 * Ordered, allows fast in-order traversal, merging and intersections
 * Stored in an Erlang binary, making parallel no-copy reads possible,
   easy storage
 * O(log n) reads

These properties makes Bisect a good fit for read-heavy
workloads. Updates to the dictionary are expensive. On commodity
multi-core machines it's possible to achieve millions of reads per
second also with more than 100M keys.

`bisect_server` is a gen_server wrapping a instance of Bisect for
parallel no-copy reads.

The API is a bit crap as it started out as a quick experiment and then
people started using it, making it difficult to warrant fixing the
API.

## Usage

When creating a new Bisect you need to decide up front on the key and
value size. This is great for storing many things of the same type,
but not so good for different types. Let's say I want to use a single
byte for both value and key, allowing me 256 unique keys.

```erlang
1> bisect:new(1, 1).
{bindict,1,1,2,<<>>}

%% Insert the byte 104 with value 10
2> bisect:insert(bisect:new(1, 1), <<104>>, <<10>>).
{bindict,1,1,2,<<"h\n">>}

3> bisect:find(v(-1), <<104>>).
<<"\n">>

%% If the input parameters have the wrong size, insertion fails
4> catch bisect:insert(bisect:new(1, 1), <<104, 101>>, <<10>>).
{'EXIT',{badarg,[{bisect,insert,3,[]},{lists,sort,2,[]}]}}

%% Serialization
5> bisect:serialize(bisect:insert(bisect:new(1, 1), <<104>>, <<10>>)).
<<131,104,5,100,0,7,98,105,110,100,105,99,116,97,1,97,1,
  97,2,109,0,0,0,2,104,10>>
6> bisect:deserialize(v(-1)).
{bindict,1,1,2,<<"h\n">>}

%% Bulk insert, much more efficient than one insert at a time
7> bisect:bulk_insert(bisect:new(1, 1), [{<<104>>, <<10>>}, {<<101>>, <<10>>}]).
{bindict,1,1,2,<<"h\ne\n">>}

%% Curious how big memory you will use?
8> bisect:expected_size(bisect:new(1, 1), 255).
510
9> bisect:expected_size_mb(bisect:new(8, 1), 10000000).
85.8306884765625
```

It is up to the user to encode/decode keys and values in a way that
makes sense, Bisect only stores the raw bytes you give as input.
