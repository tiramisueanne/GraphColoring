from pyspark import SparkContext, SparkConf
import spark
import math
import functools


def choose_node_color(color_sets, node, edges):
    for color, nodes in color_sets.items():
        if not any((k in nodes) for k in edges):
            return color


# in this bin, recolor nodes of the highest_numbered color in the bin
def recolor_bin(bin_iter, num_colors):
    bin = list(bin_iter)
    bin.sort(key=lambda x: x[0])
    if len(bin) <= num_colors:
        return []
    colors_to_keep = bin[:num_colors]
    colors_to_recolor = bin[num_colors:]

    # for colors_to_keep, create map of colors to sets of nodes (we don't need the edge lists)
    color_sets = {}
    for color, nodes in colors_to_keep:
        color_sets[color] = set(x[0] for x in nodes)

    for old_color, recolor_nodes in colors_to_recolor:
        recolored_nodes = {}
        # parallelize this later
        for node, edges in recolor_nodes:
            color = choose_node_color(color_sets, node, edges)
            if color not in recolored_nodes:
                recolored_nodes[color] = []
            recolored_nodes[color].append((node, edges))
        for new_color, new_color_nodes in colors_to_keep:
            if new_color in recolored_nodes:
                new_color_nodes.extend(recolored_nodes[color])

    return colors_to_keep


def keyPartitioner(key, partition_span):
    return key // partition_span


import spark
def kuhn_wattenhoffer(sc):
    nodes, _ = spark.create_initial_rdds(sc, "inputs/8.txt")
    color_sets = nodes.map(lambda x: (x[0], [x]))
    num_nodes = nodes.count()
    num_colors = num_nodes
    final_num_colors = int(math.log(num_nodes)) + 1

    while num_colors > final_num_colors:
        if num_colors <= final_num_colors * 2:
            num_partitions = 1
        else:
            num_partitions = math.ceil(num_colors / (final_num_colors * 2))
        partition_span = num_colors // num_partitions
        # partition into bins
        partition_func = functools.partial(keyPartitioner, partition_span=partition_span)
        color_sets = color_sets.partitionBy(num_partitions, partition_func)

        # recolor each bin
        map_func = functools.partial(recolor_bin, num_colors=final_num_colors)
        color_sets = color_sets.mapPartitions(map_func)

        num_colors = color_sets.count()

    print(color_sets.collect())


if __name__ == "__main__":
    conf, sc = spark.setup_spark_context()
    kuhn_wattenhoffer(sc)
