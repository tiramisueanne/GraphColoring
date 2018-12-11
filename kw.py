from pyspark import SparkContext, SparkConf
import spark
import math
import functools
from multiprocessing.pool import ThreadPool

def choose_node_color(edges, color_sets):
    for color, nodes in color_sets.items():
        if not any((k in nodes) for k in edges):
            return color

parallelization_threshold = 10

def recolor_nodes(color_sets, nodes):
    recolored_nodes = {}
    if False:#len(nodes) <= parallelization_threshold:
        for node, edges in nodes:
            color = choose_node_color(edges, color_set)
            if color not in recolored_nodes:
                recolored_nodes[color] = []
            recolored_nodes[color].append((node, edges))
            color_sets[color].add(node)
    else:
        # do in parallel
        pool = ThreadPool(processes=16)
        edges = list(edges for node, edges in nodes)
        choose_node_func = functools.partial(choose_node_color, color_sets=color_sets)
        colors = pool.map(choose_node_func, edges)
        pool.close()
        # add results to recolored_nodes and color_sets
        color_assignments = zip(colors, nodes)
        for color, node_tuple in color_assignments:
            if not color in recolored_nodes:
                recolored_nodes[color] = []
            recolored_nodes[color].append(node_tuple)
            color_sets[color].add(node_tuple[0])
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
        for new_color, new_color_nodes in colors_to_keep:
            if new_color in recolored_nodes:
                new_color_nodes.extend(recolored_nodes[new_color])

    return colors_to_keep


def kuhn_wattenhofer(nodes, final_num_colors):
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

        num_colors = color_sets.count()

    return color_sets.collect()


# Node is a tuple of nodes index and then a list of edges
# Then a color is the color number and a list of node tuples ( the thing above)
def run_kw(sc, filename):
    nodes, _ = spark.create_initial_rdds(sc, filename)
    # just to reflect how we created our sample graphs
    num_colors = int(math.log(nodes.count())) + 1
    coloring = kuhn_wattenhoffer(nodes, num_colors)
    return coloring


if __name__ == "__main__":
    conf, sc = spark.setup_spark_context()
    run_kw(sc)



