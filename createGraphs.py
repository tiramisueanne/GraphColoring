#
import sys
import math
import random

def random_edges(num_nodes):
    max_degree = math.floor(math.log(num_nodes))
    graph = [[] for i in range(num_nodes)]
    for i in range(num_nodes):
        while len(graph[i]) < max_degree:
            r = random.randint(0, num_nodes - 1)
            if r not in graph[i]:
                graph[i].append(r)
                graph[r].append(i)
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
