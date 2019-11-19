import os
import pathlib
import time
import types

from ck.connection import http
from ck.connection import process
from ck.connection import ssh


_dir_path = pathlib.Path(__file__).parent
_ck_path = _dir_path.joinpath('clickhouse')
_config_path = _dir_path.joinpath('config.xml')
_users_path = _dir_path.joinpath('users.xml')


class QueryError(RuntimeError):
    pass


class ServiceError(RuntimeError):
    pass


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
        gen_stderr
    ):
        assert type(gen_stdin) is types.GeneratorType
        assert type(gen_stdout) is types.GeneratorType
        assert type(gen_stderr) is types.GeneratorType
        assert method in {'tcp', 'http', 'ssh'}

        if method == 'tcp':
            join_raw = process.run(
                [
                    str(_ck_path),
                    'client',
                    f'--host={self._host}',
                    f'--port={self._tcp_port}',
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
                '/',
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
                    str(_ck_path),
                    'client',
                    f'--port={self._tcp_port}',
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
        gen_out=None
    ):
        assert type(query_text) is str
        assert method in {'tcp', 'http', 'ssh'}
        assert type(use_async) is bool
        assert gen_in is None or type(gen_in) is types.GeneratorType
        assert gen_out is None or type(gen_out) is types.GeneratorType

        def make_stdin():
            yield f'{query_text}\n'.encode()

            if gen_in is not None:
                yield from gen_in

        stdout_list = []

        def make_stdout():
            if gen_out is None:
                while True:
                    stdout_list.append((yield))
            else:
                yield from gen_out

        stderr_list = []

        def make_stderr():
            while True:
                stderr_list.append((yield))

        join_raw = self._run(
            method,
            make_stdin(),
            make_stdout(),
            make_stderr()
        )

        def join():
            if not join_raw():
                raise QueryError(
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
        except QueryError:
            return False


class LocalSession(PassiveSession):
    def __init__(
        self,
        tcp_port=9000,
        http_port=8123,
        ssh_port=22,
        ssh_username=None,
        ssh_password=None,
        ssh_public_key=None,
        ssh_command_prefix=None,
        path=str(pathlib.Path().cwd().joinpath('data')),
        config={'listen_host': '0.0.0.0'},
        stop=False,
        start=True,
        ping_interval=0.1,
        ping_retry=100
    ):
        assert type(tcp_port) is int
        assert type(http_port) is int
        assert type(ssh_port) is int
        assert ssh_username is None or type(ssh_username) is str
        assert ssh_password is None or type(ssh_password) is str
        assert ssh_public_key is None or type(ssh_public_key) is str
        assert ssh_command_prefix is None or type(ssh_command_prefix) is str
        assert type(path) is str
        assert type(config) is dict
        for key, value in config.items():
            assert type(key) is str
            assert type(value) is str
        assert type(stop) is bool
        assert type(start) is bool
        assert type(ping_interval) is int or type(ping_interval) is float
        assert type(ping_retry) is int

        super().__init__(
            'localhost',
            tcp_port,
            http_port,
            ssh_port,
            ssh_username,
            ssh_password,
            ssh_public_key,
            ssh_command_prefix
        )

        self._path = pathlib.Path(path)
        self._config = config

        if stop:
            self.stop(ping_interval, ping_retry)

        if start:
            self.start(ping_interval, ping_retry)

    def get_pid(
        self
    ):
        try:
            pid_path = self._path.joinpath('pid')
            pid = int(open(pid_path, 'r').read().strip())
        except FileNotFoundError:
            return

        try:
            os.kill(pid, 0)
        except OSError:
            return

        return pid

    def start(
        self,
        ping_interval=0.1,
        ping_retry=100
    ):
        assert type(ping_interval) is int or type(ping_interval) is float
        assert type(ping_retry) is int

        pid = self.get_pid()

        if pid is not None:
            return

        def make_stdin():
            yield from ()

        def make_stdout():
            yield

        def make_stderr():
            yield

        pid_path = self._path.joinpath('pid')
        tmp_path = self._path.joinpath('tmp')
        format_schema_path = self._path.joinpath('format_schema')
        user_files_path = self._path.joinpath('user_files')
        # notice: log_path and errorlog_path does not work
        log_path = self._path.joinpath('log')
        errorlog_path = self._path.joinpath('errorlog')

        if process.run(
            [
                str(_ck_path),
                'server',
                '--daemon',
                f'--config-file={_config_path}',
                f'--pid-file={pid_path}',
                '--',
                f'--tcp_port={self._tcp_port}',
                f'--http_port={self._http_port}',
                f'--users_config={_users_path}',
                f'--path={self._path}',
                f'--tmp_path={tmp_path}',
                f'--format_schema_path={format_schema_path}',
                f'--user_files_path={user_files_path}',
                f'--logger.log={log_path}',
                f'--logger.errorlog={errorlog_path}',
                '--mark_cache_size=5368709120',
                *(
                    f'--{key}={value}'
                    for key, value in self._config.items()
                )
            ],
            make_stdin(),
            make_stdout(),
            make_stderr()
        )():
            raise ServiceError(self._host)

        for i in range(ping_retry):
            pid = self.get_pid()

            if pid is not None:
                break

            time.sleep(ping_interval)
        else:
            raise ServiceError(self._host)

        while not self.ping():
            time.sleep(ping_interval)

            if self.get_pid() is None:
                raise ServiceError(self._host)

        return pid

    def stop(
        self,
        ping_interval=0.1,
        ping_retry=100
    ):
        assert type(ping_interval) is int or type(ping_interval) is float
        assert type(ping_retry) is int

        pid = self.get_pid()

        if pid is None:
            return

        os.kill(pid, 15)

        for i in range(ping_retry):
            if self.get_pid() is None:
                break

            time.sleep(ping_interval)
        else:
            os.kill(pid, 9)

            while self.get_pid() is None:
                time.sleep(ping_interval)

        return pid
