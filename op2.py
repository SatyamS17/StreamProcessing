#!/usr/bin/env python3

import sys

key = sys.argv[1]
value = sys.argv[2]
stage = sys.argv[3]
fileName = sys.argv[4]

file = None
try:
    file = open(fileName, "a")
    file.write(str(stage) + ":" + str(key) + "+1\n")
finally:
    if file is not None:
        file.close()

print(key)
print(value+"asdf")