from ck.connection import http
from ck.connection import process
from ck.connection import ssh


run_http = http.run_http  # pylint: disable=invalid-name
run_process = process.run_process  # pylint: disable=invalid-name
connect_ssh = ssh.connect_ssh  # pylint: disable=invalid-name
run_ssh = ssh.run_ssh  # pylint: disable=invalid-name
