import sys
import math
import random

#
# Each connected word represents an integer of the following layout
# number_of_nodes
# index_of_node index_of_ending_node_in_edge index_of_ending_node index_of_ending_node
# ''
def random_edges(num_nodes):
    max_degree = math.floor(math.log(num_nodes))
    print(max_degree)
    graph = [[] for i in range(num_nodes)]
    for j in range(max_degree):
        all_nodes = set(range(num_nodes))
        while len(all_nodes) > 0:
            # choose a random pair of nodes
            edge = random.sample(all_nodes, 2)
            # if that edge already exists, try again
            while edge[0] in graph[edge[1]]:
                edge = random.sample(all_nodes, 2)
                # add an edge between those nodes
            n1 = edge[0]
            n2 = edge[1]
            graph[n1].append(n2)
            graph[n2].append(n1)
            all_nodes.remove(n1)
            all_nodes.remove(n2)
    return graph


def main():
    if len(sys.argv) < 3:
        print("need two command-line arguments: number of nodes, output file name")
        return
    num_nodes = int(sys.argv[1])
    graph = random_edges(num_nodes)
    filename = sys.argv[2]
    with open(filename, 'w') as file:
        file.write(str(num_nodes) + "\n")
        for i in range(num_nodes):
            file.write(str(i) + " ")
            for j in graph[i][:-1]:
                file.write(str(j) + " ")
            file.write(str(graph[i][-1]) + "\n")


if __name__ == "__main__":
    main()
