import subprocess
import sys

from ck.clickhouse import lookup


subprocess.Popen([lookup.binary_file(), *sys.argv[1:]]).wait()
