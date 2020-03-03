import subprocess
import sys

from ck.clickhouse import lookup


process = subprocess.Popen([lookup.binary_file(), *sys.argv[1:]])
process.wait()
