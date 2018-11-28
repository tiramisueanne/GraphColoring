import createGraphs
# Given a file path, read in the file
def read_in_files():
    print("hello!")
# Compute a coloring given the naive algorithm in the Bae paper
def naive_color(list_of_lists):
    # Fill with initial colorings
    colors = [x for x in range(len(list_of_lists))]
    # Find the max degree (length of list of edges for a node)
    max_degree = max([len(x) for x in list_of_lists])
    # Reduce coloring stage
    for j in range(max_degree + 1, len(list_of_lists)):
        # Check which degrees
        for k in range(max_degree + 1):
            # If the candidate node does not contain our current
            # index in its list of end nodes
            if j not in list_of_lists[k]:
                colors[j] = k
                break;

    # The resulting color of each node i is stored in colors[i]
    return colors

def check_naive():
    fake_graph = [[1, 2], [2,0], [0,1]]
    colors = naive_color(fake_graph)
    print(colors)
    fake_graph = [[1], [0], []]
    colors = naive_color(fake_graph)
    print(colors)

if __name__ == "__main__":
    check_naive()
