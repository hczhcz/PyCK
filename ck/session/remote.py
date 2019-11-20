import pathlib
import time

from ck import clickhouse
from ck import exception
from ck import generator
from ck.connection import ssh
from ck.session import passive


class RemoteSession(passive.PassiveSession):
    def __init__(
        self,
        host='localhost',
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
        assert type(host) is str
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
            host,
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
        pid_path = self._path.joinpath('pid')

        stdout_list = []

        self._connect_ssh()

        if ssh.run(
            self._ssh_client,
            [
                'cat',
                str(pid_path),
            ],
            generator.make_empty_in(),
            generator.make_collect_out(stdout_list),
            generator.make_ignore_out()
        )():
            return

        pid = int(b''.join(stdout_list).decode().strip())

        if ssh.run(
            self._ssh_client,
            [
                'kill',
                '-0',
                str(pid),
            ],
            generator.make_empty_in(),
            generator.make_empty_out(),
            generator.make_ignore_out()
        )():
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

        pid_path = self._path.joinpath('pid')
        tmp_path = self._path.joinpath('tmp')
        format_schema_path = self._path.joinpath('format_schema')
        user_files_path = self._path.joinpath('user_files')
        # notice: log_path and errorlog_path does not work
        log_path = self._path.joinpath('log')
        errorlog_path = self._path.joinpath('errorlog')

        if ssh.run(
            self._ssh_client,
            [
                *(
                    []
                    if self._ssh_command_prefix is None
                    else [self._ssh_command_prefix]
                ),
                str(clickhouse.binary_path),
                'server',
                '--daemon',
                f'--config-file={clickhouse.config_path}',
                f'--pid-file={pid_path}',
                '--',
                f'--tcp_port={self._tcp_port}',
                f'--http_port={self._http_port}',
                f'--users_config={clickhouse.users_path}',
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
                ),
            ],
            generator.make_empty_in(),
            generator.make_empty_out(),
            generator.make_empty_out()
        )():
            raise exception.ServiceError(self._host)

        for i in range(ping_retry):
            pid = self.get_pid()

            if pid is not None:
                break

            time.sleep(ping_interval)
        else:
            raise exception.ServiceError(self._host)

        while not self.ping():
            time.sleep(ping_interval)

            if self.get_pid() is None:
                raise exception.ServiceError(self._host)

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

        if ssh.run(
            self._ssh_client,
            [
                'kill',
                '-15',
                str(pid),
            ],
            generator.make_empty_in(),
            generator.make_empty_out(),
            generator.make_ignore_out()
        )():
            raise exception.ServiceError(self._host)

        for i in range(ping_retry):
            if self.get_pid() is None:
                break

            time.sleep(ping_interval)
        else:
            if ssh.run(
                self._ssh_client,
                [
                    'kill',
                    '-9',
                    str(pid),
                ],
                generator.make_empty_in(),
                generator.make_empty_out(),
                generator.make_ignore_out()
            )():
                raise exception.ServiceError(self._host)

            while self.get_pid() is None:
                time.sleep(ping_interval)

        return pid
