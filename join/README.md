Joining datasets in mapreduce
=

For this training, we are going to play with two public datasets:
* World disasters in the world since 2000
* Human development index (hdi) for countries

Deciding what is the best join strategy for your job, depends in many factors, here we are introducing the most common ways of joining in mapreduce, we use python through Dumbo, but the implementation in other frameworks or languages shouldnt be different.

1. Reduce side Join
==

This approach takes the 2 datasets as input, partitions them using the key we want to join with for each of them (yields as key in the mapper the "joinkey"), and combines them in the reducer (we would have flagged each value to specifiy the feed it's coming from).

In base_join.py we implement manually this type of join, run it:

```
dumbo start base_join.py -input ../dat/disasters/disasters.tsv -input ../dat/hdi/hdi.tsv -output output1 -overwrite yes -hadoop /opt/hadoop
```

We can make this cleaner by using BinaryPartitioner, implemented in dumbo with joinkeys through primary and secondary decorator, also, we use a MultiMapper to make code cleaner and easier to understand (keys_join.py), the approach is similar, but the performance is much better, since we are partitioning through the mapreduce core, instead of in the application


```
dumbo start keys_join.py -input ../dat/disasters/disasters.tsv -input ../dat/hdi/hdi.tsv -output output2 -overwrite yes -hadoop /opt/hadoop
```

2. Map side Join
==

If one of the datasets is small, and can fit in memory, a better approach can be to broadcast that dataset to each of the nodes (using the distributed cache), and make them cache that dataset in memory. The large dataset will be processed using a mapper, and querying the lookup previously loaded.

```
dumbo start map_join.py -input ../dat/disasters/disasters.tsv -file ../dat/hdi/hdi.tsv -output output3 -overwrite yes -hadoop /opt/hadoop
```

As we are doing the join in the map, we need no reducer, improving performace since shuffle and reduce phases are not run.

If we know a little about the nature on the dataset and on how is it generate, it might be usefull to store last search, to prevent from querying the lookup with each rerecord

```
dumbo start map_join2.py -input ../dat/disasters/disasters.tsv -file ../dat/hdi/hdi.tsv -output output4 -overwrite yes -hadoop /opt/hadoop
```

3. Reduce side cached-Join
==

A similar approach to 2 (caching one of the datasets) can be implemented on the reducer.

```
dumbo start reducelkp_join.py -input ../dat/disasters/disasters.tsv -file ../dat/hdi/hdi.tsv -output output5 -overwrite yes -hadoop /opt/hadoop
```

This way, we are reducing the number of queries we do to the lookup, but we need to pass both datasets through the shuffle and reduce phases, usually, we are not just joining, but also doing any other type of transformations that requires to shuffle and reduce, if that is the case, and one dataset is small, it might be usefull to follow this strategy.


