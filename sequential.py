import create_graphs
import test

# Given a file path, read in the file
def read_in_files():
    print("hello!")

# Compute a coloring given the naive algorithm in the Bae paper
def naive_color(edge_lists):
    # Fill with initial colorings
    # colors = [x for x in range(len(edge_lists))]
    # Find the max degree (length of list of edges for a node)
    max_degree = max([len(x) for x in edge_lists])
    color_sets = [set([x]) for x in range(max_degree+1)]
    # Reduce coloring stage
    for j in range(max_degree + 1, len(edge_lists)):
        # Check which degrees
        for c in range(max_degree + 1):
            # If the candidate node does not contain our current
            # index in its list of end nodes
            if not any((k in color_sets[c]) for k in edge_lists[j]):
                color_sets[c].add(j)
                break;

    # Each node is in a list with non-edge connected nodes
    return color_sets

# Similar to naive without as much work prepping it
def combine_colors(color_sets, edge_lists, max_degree):
    for j in range(max_degree + 1, len(color_sets)):
        # Do the same thing as naive, pretty much
        for c in range(max_degree + 1):
            if not any((k in color_sets[c]) for k in edge_lists[j]):
                color_sets[c].add(j)
                break
    # Return the first max_degree + 1 colors
    return color_sets[:max_degree + 2]

def kw_color(edge_lists):
    max_degree = max([len(x) for x in edge_lists])
    # This will get us all of the starting indexes of bins
    start_indexes = [x for x in range(len(edge_lists)) if (x % (2 * (max_degree + 1)) == 0 and x + 2 * (max_degree + 1) <= len(edge_lists))]
    color_sets = []
    for i in range(len(start_indexes)):
        curr_start = start_indexes[i]
        next_start = start_indexes[i+1] if i + 1 != len(start_indexes) else len(start_indexes)
        color_sets.extend(naive_color(edge_lists[curr_start:next_start]))
    # Now we change to merging colors rather than individual nodes
    while(len(color_sets) > max_degree + 1):
        start_indexes = [x for x in range(len(color_sets)) if (x % (2 * (max_degree + 1)) == 0)]
        new_sets = []
        # Essentially do the same reduction again, but this time
        # we rid ourselves of the previous color_set  at the end of the loop
        # because it has stale colors
        for i in range(len(start_indexes)):
            curr_start = start_indexes[i]
            next_start = start_indexes[i + 1] if i + 1 != len(start_indexes) else len(start_indexes)
            new_sets.extend(combine_colors(color_sets[curr_start:next_start], edge_lists, max_degree))
        color_sets = new_sets
        new_sets = []
    return color_sets

def check_naive():
    fake_graph = [[1, 2], [2,0], [0,1]]
    colors = naive_color(fake_graph)
    print(test.check_coloring(fake_graph, colors))
    # print(colors)
    fake_graph = [[1], [0], []]
    colors = naive_color(fake_graph)
    print(test.check_coloring(fake_graph, colors))
    fake_graph = [[1], [2], [3], [0], [1], [2]]
    colors = kw_color(fake_graph)
    print(test.check_coloring(fake_graph, colors))
    fake_graph = [[1], [0], []]
    colors = kw_color(fake_graph)
    print(test.check_coloring(fake_graph, colors))
    fake_graph = [[1, 2], [2,0], [0,1]]
    colors = kw_color(fake_graph)
    print(test.check_coloring(fake_graph, colors))



if __name__ == "__main__":
    check_naive()
