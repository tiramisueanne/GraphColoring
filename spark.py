import sys
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
    edges = [int(num_str) for num_str in number_strings]
    return (key, edges)


def assign_initial_color(node):
    index = node[0]
    return (index, set([index]))


def create_initial_rdds(sc, filename):
    text_file = sc.textFile(filename)
    nodes = text_file.map(format_text_line).filter(lambda item: item is not None).cache()
    colors = nodes.map(assign_initial_color).cache()
    return nodes, colors


if __name__ == '__main__':
    conf, sc = setup_spark_context()
    if len(sys.argv) < 2:
        print("Argument missing: input file path")
        exit()
    filename = sys.argv[1]
    nodes, colors = create_initial_rdds(conf, sc, filename)
