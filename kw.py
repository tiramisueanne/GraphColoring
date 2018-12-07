from pyspark import SparkContext, SparkConf
import spark
import math
import functools


def choose_node_color(color_sets, node, edges):
    for color, nodes in color_sets.items():
        if not any((k in nodes) for k in edges):
            return color

parallelization_threshold = 10

def recolor_nodes(color_sets, nodes):
    recolored_nodes = {}
    if True:# len(nodes) <= parallelization_threshold:
        for node, edges in nodes:
            color = choose_node_color(color_sets, node, edges)
            if color not in recolored_nodes:
                recolored_nodes[color] = []
            recolored_nodes[color].append((node, edges))
    else:
        # parallelize this
        pass
    return recolored_nodes


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

    for old_color, nodes_to_recolor in colors_to_recolor:
        recolored_nodes = recolor_nodes(color_sets, nodes_to_recolor)
        print(recolored_nodes)
        for new_color, new_color_nodes in colors_to_keep:
            if new_color in recolored_nodes:
                new_color_nodes.extend(recolored_nodes[new_color])

    return colors_to_keep


def kuhn_wattenhoffer(nodes, final_num_colors):
    num_nodes = nodes.count()
    color_sets = nodes.map(lambda x: (x[0], [x]))

    num_colors = num_nodes
    while num_colors > final_num_colors:
        # partition into bins
        num_partitions = math.ceil(num_colors / (final_num_colors * 2))
        #SUEDO: ask if this will give us the correct number in bins,
        # or if that is at all necessary
        color_sets = color_sets.partitionBy(int(num_partitions))

        # recolor each bin
        map_func = functools.partial(recolor_bin, num_colors=final_num_colors)
        color_sets = color_sets.mapPartitions(map_func)

        print(color_sets.collect())

        num_colors = color_sets.count()

    return color_sets.collect()


# Node is a tuple of nodes index and then a list of edges
# Then a color is the color number and a list of node tuples ( the thing above)
def run_kw(sc):
    nodes, _ = spark.create_initial_rdds(sc, "inputs/8.txt")
    # just to reflect how we created our sample graphs
    num_colors = int(math.log(nodes.count())) + 1
    coloring = kuhn_wattenhoffer(nodes, num_colors)
    print(coloring)


if __name__ == "__main__":
    conf, sc = spark.setup_spark_context()
    run_kw(sc)
