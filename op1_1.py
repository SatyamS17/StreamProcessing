#!/usr/bin/env python3

"""
Filter all lines in the Traffic Signs dataset that contain pattern 'X' 
and return only the columns 'OBJECTID, Sign_Type' to the output; X is 
a parameter that the TA will tell you.
"""

import sys

key = sys.argv[1]
value = sys.argv[2]
pattern = sys.argv[3]

if pattern in value:
    print(key) # Key
    print(value) # Value