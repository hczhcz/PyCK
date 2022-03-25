import os
import sys

from ck.clickhouse import lookup


os.execv(lookup.binary_file(), sys.argv)
