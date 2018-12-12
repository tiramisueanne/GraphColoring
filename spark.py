import sys
import kw
import math
from pyspark import SparkContext, SparkConf

def setup_spark_context():
    conf = SparkConf().setAppName('Kuhn-Wattenhofer')
    sc = SparkContext(conf=conf)
    return conf, sc

def format_text_line(line):
    # split and parse ints
    number_strings = line.split(' ')
    if len(number_strings) < 2:
        return None
    key = int(number_strings[0])
    edges = [int(num_str) for num_str in number_strings[1:]]
    return (key, edges)

def assign_initial_color(node):
    index = node[0]
    return (index, set([index]))

def create_initial_rdd(sc, filename):
    text_file = sc.textFile(filename)
    nodes = text_file.map(format_text_line).filter(lambda item: item is not None).cache()
    return nodes

def main(sc, filename, num_runs):
    nodes = create_initial_rdd(sc, filename)
    num_colors = int(math.log(nodes.count())) + 1 # cheating cause we know this is max_degree + 1
    for i in range(num_runs):
        coloring = kw.kuhn_wattenhofer(nodes, num_colors)
        if len(coloring) > num_colors:
            print("Ahh help!")
            break
        print("Run " + str(i) + " complete")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Argument missing: input file path")
        exit()
    filename = sys.argv[1]
    if len(sys.argv) > 2:
        num_runs = int(sys.argv[2])
    else:
        num_runs = 1
    conf, sc = setup_spark_context()
    main(sc, filename, num_runs)
