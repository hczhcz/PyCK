import posix
import sys

from ck.clickhouse import lookup


posix.execv(lookup.binary_file(), sys.argv)
