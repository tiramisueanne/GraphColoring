from pyspark import SparkContext, SparkConf
import spark

def is_valid_node_coloring(item):
    node_pair, color_pair = item
    node, edges = node_pair
    color, color_set = color_pair
    for edge in edges:
        if edge in color_set:
            return False
    return True


def reformat_coloring_pair(item):
    node_pair, color_pair = item
    node, edges = node_pair
    color, color_set = color_pair
    return (node, color)


# color_assignments: RDD with elements (color, node)
# edges: RDD with elements (node, [neighbor1, neighbor2, ...])
def reduce_bin(color_assignments, edges, start_color, end_color):
    split_color = (end_color - start_color)//2

    # find current color_sets of colors we plan to keep
    # (color, {node1, node2, ...})
    color_sets = color_assignments.filter(lambda x: x[0] < split_color).groupByKey().map(lambda x: (x[0], set(x[1])))
    for c in range(split_color, end_color):
        # get an rdd of the form (node, [neighbor1, neighbor2, ...])
        # for all nodes of color c (old color to eliminate)
        color_nodes = color_assignments.filter(lambda x: x[0] == c).join(edges).mapValues(lambda x: x[1])

        # get all possible color assignments for these nodes
        # ((node, [neighbor1, neighbor2, ...]), (color, {node1, node2}))
        possible_assignments = color_nodes.cartesian(color_sets)

        # filter for valid color assignments
        # (node, color)
        valid_assignments = possible_assignments.filter(is_valid_node_coloring).map(reformat_coloring_pair)

        # pick lowest valid assignment for each node
        #print(valid_assignments.collect())
        new_assignments = valid_assignments.groupByKey()
        new_assignments = new_assignments.mapValues(min)
        # flip the pairs around (from (node, color) to (color, node))
        new_assignments.foreach(lambda x: (x[1], x[0]))

        new_assignments = color_sets.cogroup(new_assignments).flatMapValues(lambda x: set(x[0]).union(x[1]))

        # join this with existing color assignments rdd
        # (leave the old assignments in for now, filter them out at the end)
        color_sets = color_assignments.filter(lambda x : x[0] < split_color).cogroup(new_assignments).flatMapValues(lambda x: x).map(lambda x: (x[0], set(x[1])))
        color_assignments = color_sets.flatMapValues(lambda x: x)
        print("Color sets: ", color_sets.collect(), "Color assign: ", color_assignments.collect())

    # filter the old colors out of color_assignments
    color_assignments = color_assignments.filter(lambda x: x[0] < split_color or x[0] >= end_color)
    # return the modified color assignments
    return color_assignments

import spark
def try_reduce_bin(sc):
    nodes, _ = spark.create_initial_rdds(sc, "inputs/8.txt")
    color_assignments = nodes.map(lambda x: (x[0], x[0]))
    color_assignments = reduce_bin(color_assignments, nodes, 0, 8)
    print(color_assignments.collect())

if __name__ == "__main__":
    conf, sc = spark.setup_spark_context()
    try_reduce_bin(sc)
