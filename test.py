import create_graphs
import sequential

def check_coloring(graph, coloring):
    max_degree = max([len(x) for x in graph])
    if max_degree + 1 < len(coloring):
        return False
    for color_set in coloring:
        for v in color_set:
            if any((u in color_set) for u in graph[v]):
                return False
    return True



def test_coloring_method(filename, coloring_method):
    graph = create_graphs.read_graph_file(filename)
    print(str(len(graph)) + " nodes....", end='.')
    coloring = coloring_method(graph)
    if check_coloring(graph, coloring):
        print("Pass")
    else:
        print("Fail")


if __name__ == '__main__':
    directory = "inputs/"
    filenames = ["8.txt",
                 "32.txt",
                 "128.txt",
                 "512.txt",
                 "2048.txt"]
    for filename in filenames:
        test_coloring_method(directory + filename, sequential.naive_color)
