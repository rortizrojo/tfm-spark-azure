#! /usr/bin/python

import json
import sys
# from pprint import pprint


def main(argv):
    json_file = argv[0]


    with open(json_file) as json_data:
        data = json.load(json_data)
        print(data[0]['value'])


if __name__ == "__main__":
   main(sys.argv[1:])