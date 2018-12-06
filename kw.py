from pyspark import SparkContext, SparkConf
import spark
import math
import functools


def choose_node_color(colors, node, edges):
    for color, nodes in colors:
        color_set = map(lambda x: x[0], nodes)
        if not any((k in color_set) for k in edges):
            return color


# in this bin, recolor nodes of the highest_numbered color in the bin
def recolor(bin_iter, n, num_colors):
    bin = list(bin_iter)
    bin.sort(key=lambda x: x[0])
    if len(bin) <= num_colors:
        return []
    colors_to_keep = bin[:num_colors]
    color_to_recolor = bin[-1]
    nodes_to_recolor = color_to_recolor[1]

    recolored_nodes = []#{}
    # parallelize this later
    for node, edges in nodes_to_recolor:
        color = choose_node_color(colors_to_keep, node, edges)
        recolored_nodes.append((color, (node, edges)))
    return recolored_nodes


def keyPartitioner(key, partition_span):
    return key // partition_span


import spark
def kuhn_wattenhoffer(sc):
    nodes, _ = spark.create_initial_rdds(sc, "inputs/8.txt")
    color_assignments = nodes.map(lambda x: (x[0], (x[0], x[1])))
    num_nodes = nodes.count()
    num_colors = num_nodes
    final_num_colors = int(math.log(num_nodes)) + 1

    while num_colors > final_num_colors:
        if num_colors <= final_num_colors * 2:
            num_partitions = 1
        else:
            num_partitions = math.ceil(num_colors / (final_num_colors * 2))
        # partition into bins
        partition_span = num_nodes // num_partitions
        partition_func = functools.partial(keyPartitioner, partition_span=partition_span)
        color_assignments = color_assignments.partitionBy(num_partitions, partitionFunc=partition_func)
        print(color_assignments.glom().collect())

        # for each bin, loop through colors and recolor those nodes
        for i in range(math.ceil(num_colors / num_partitions) - final_num_colors):
            print(i)
            # get color sets
            color_sets = color_assignments.groupByKey().partitionBy(num_partitions, partitionFunc=partition_func)
            # recolor nodes of the highest-valued color in each bin
            map_func = functools.partial(recolor, n=i, num_colors=final_num_colors)
            recolored_nodes = color_sets.mapPartitions(map_func)
            print(recolored_nodes.collect())
            # filter out old colors from color_assignments
            # and union with newly colored nodes
            # TODO: fix cause it's broken
            color_assignments = color_assignments.filter(lambda x: x[0] % partition_span != final_num_colors + i).union(recolored_nodes)
            print(color_assignments.collect())

        num_colors = nodes.count()

    print(color_assignments.collect())


def recolor_something():
    bin = []
    bin.append((0, [(1,[]),(2,[]),(3,[])]))
    bin.append((1, [(4,[]),(5,[]),(6,[])]))
    bin.append((3, [(9,[1,2]),(10,[4,5])]))
    bin.append((2, [(7,[1,2]),(8,[4,5])]))

    print(recolor(bin, 0, 2))
    print(recolor(bin, 1, 2))


if __name__ == "__main__":
    conf, sc = spark.setup_spark_context()
    kuhn_wattenhoffer(sc)
