import itertools
import types
import urllib.parse

from ck import clickhouse
from ck import exception
from ck import iteration
from ck.connection import http
from ck.connection import process
from ck.connection import ssh


class PassiveSession(object):
    def __init__(
        self,
        host='localhost',
        tcp_port=9000,
        http_port=8123,
        ssh_port=22,
        ssh_username=None,
        ssh_password=None,
        ssh_public_key=None,
        ssh_command_prefix=None
    ):
        assert type(host) is str
        assert type(tcp_port) is int
        assert type(http_port) is int
        assert type(ssh_port) is int
        assert ssh_username is None or type(ssh_username) is str
        assert ssh_password is None or type(ssh_password) is str
        assert ssh_public_key is None or type(ssh_public_key) is str
        assert ssh_command_prefix is None or type(ssh_command_prefix) is str

        self._host = host
        self._tcp_port = tcp_port
        self._http_port = http_port
        self._ssh_port = ssh_port
        self._ssh_username = ssh_username
        self._ssh_password = ssh_password
        self._ssh_public_key = ssh_public_key
        self._ssh_command_prefix = ssh_command_prefix
        self._ssh_client = None

    def _connect_ssh(
        self
    ):
        if self._ssh_client is None:
            self._ssh_client = ssh.connect(
                self._host,
                self._ssh_port,
                self._ssh_username,
                self._ssh_password,
                self._ssh_public_key
            )

    def _run(
        self,
        method,
        gen_stdin,
        gen_stdout,
        gen_stderr,
        settings={}
    ):
        assert method in {'tcp', 'http', 'ssh'}
        assert type(gen_stdin) is types.GeneratorType
        assert type(gen_stdout) is types.GeneratorType
        assert type(gen_stderr) is types.GeneratorType
        assert type(settings) is dict
        for key, value in settings.items():
            assert type(key) is str
            assert type(value) is str

        # TODO: check setting keys

        if method == 'tcp':
            join_raw = process.run(
                [
                    str(clickhouse.binary_path),
                    'client',
                    f'--host={self._host}',
                    f'--port={self._tcp_port}',
                    *(
                        f'--{key}={value}'
                        for key, value in settings.items()
                    ),
                ],
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            ok = 0
        elif method == 'http':
            join_raw = http.run(
                self._host,
                self._http_port,
                f'/?{urllib.parse.urlencode(settings)}',
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            ok = 200
        elif method == 'ssh':
            self._connect_ssh()

            join_raw = ssh.run(
                self._ssh_client,
                [
                    *(
                        []
                        if self._ssh_command_prefix is None
                        else [self._ssh_command_prefix]
                    ),
                    str(clickhouse.binary_path),
                    'client',
                    f'--port={self._tcp_port}',
                    *(
                        f'--{key}={value}'
                        for key, value in settings.items()
                    ),
                ],
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            ok = 0

        def join():
            return join_raw() == ok

        return join

    def query(
        self,
        query_text,
        method='http',
        use_async=False,
        gen_in=None,
        gen_out=None,
        settings={}
    ):
        assert type(query_text) is str
        assert method in {'tcp', 'http', 'ssh'}
        assert type(use_async) is bool
        assert gen_in is None or type(gen_in) is types.GeneratorType
        assert gen_out is None or type(gen_out) is types.GeneratorType
        assert type(settings) is dict
        for key, value in settings.items():
            assert type(key) is str
            assert type(value) is str

        stdout_list = []
        stderr_list = []

        if gen_in is None:
            gen_stdin = iteration.make_given_in(f'{query_text}\n'.encode())
        else:
            gen_stdin = itertools.chain(
                iteration.make_given_in(f'{query_text}\n'.encode()),
                gen_in
            )

        if gen_out is None:
            gen_stdout = iteration.make_collect_out(stdout_list)
        else:
            gen_stdout = gen_out

        join_raw = self._run(
            method,
            gen_stdin,
            gen_stdout,
            iteration.make_collect_out(stderr_list),
            settings
        )

        def join():
            if not join_raw():
                raise exception.QueryError(
                    self._host,
                    query_text,
                    b''.join(stderr_list).decode()
                )

            if gen_out is None:
                return b''.join(stdout_list)

        if use_async:
            return join

        return join()

    def ping(
        self,
        method='http'
    ):
        assert method in {'tcp', 'http', 'ssh'}

        try:
            return self.query('select 42', method=method) == b'42\n'
        except ConnectionError:
            return False
        except exception.QueryError:
            return False
