#!/usr/bin/env python3

import sys

from testbed.analysis import analyse

def main():
    directory = "../discv5-test"
    if len(sys.argv) > 1:
        directory = sys.argv[1]
    analyse(directory)

if __name__ == '__main__':
    main()
