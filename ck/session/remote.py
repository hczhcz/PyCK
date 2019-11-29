import pathlib
import time

from ck import connection
from ck import exception
from ck import iteration
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
        ssh_command_prefix=[],
        data_dir=None,
        config={},
        stop=False,
        start=True
    ):
        assert type(host) is str
        assert type(tcp_port) is int
        assert type(http_port) is int
        assert type(ssh_port) is int
        assert ssh_username is None or type(ssh_username) is str
        assert ssh_password is None or type(ssh_password) is str
        assert ssh_public_key is None or type(ssh_public_key) is str
        assert type(ssh_command_prefix) is list
        for arg in ssh_command_prefix:
            assert type(arg) is str
        assert data_dir is None or type(data_dir) is str
        # notice: recursive type checking
        assert type(config) is dict
        assert type(stop) is bool
        assert type(start) is bool

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

        self._require_ssh()

        if data_dir is None:
            self._path = pathlib.Path(self._ssh_default_data_dir)
        else:
            self._path = pathlib.Path(data_dir)

        self._config = config

        if stop:
            self.stop()

        if start:
            self.start()

    def get_pid(self):
        pid_path = self._path.joinpath('pid')

        # get pid

        stdout_list = []

        if connection.run_ssh(
            self._ssh_client,
            [
                'cat',
                str(pid_path),
            ],
            iteration.empty_in(),
            iteration.collect_out(stdout_list),
            iteration.ignore_out()
        )():
            return

        pid = int(b''.join(stdout_list).decode().strip())

        # find process

        if connection.run_ssh(
            self._ssh_client,
            [
                'kill',
                '-0',
                str(pid),
            ],
            iteration.empty_in(),
            iteration.empty_out(),
            iteration.ignore_out()
        )():
            return

        return pid

    def start(self, ping_interval=0.1, ping_retry=50):
        assert type(ping_interval) is int or type(ping_interval) is float
        assert type(ping_retry) is int

        pid = self.get_pid()

        if pid is not None:
            return

        config_path = self._path.joinpath('config.xml')
        pid_path = self._path.joinpath('pid')

        # create dir

        stderr_list = []

        if connection.run_ssh(
            self._ssh_client,
            [
                'mkdir',
                '--parents',
                str(self._path),
            ],
            iteration.empty_in(),
            iteration.empty_out(),
            iteration.collect_out(stderr_list)
        )():
            raise exception.ShellError(
                self._host,
                b''.join(stderr_list).decode()
            )

        # setup

        stderr_list = []

        if connection.run_ssh(
            self._ssh_client,
            [
                *self._ssh_command_prefix,
                'python3',
                '-m',
                'ck.clickhouse.setup',
            ],
            iteration.given_in(repr({
                'tcp_port': self._tcp_port,
                'http_port': self._http_port,
                'data_dir': str(self._path),
                'config': self._config,
            }).encode()),
            iteration.empty_out(),
            iteration.collect_out(stderr_list)
        )():
            raise exception.ShellError(
                self._host,
                b''.join(stderr_list).decode()
            )

        # run

        if connection.run_ssh(
            self._ssh_client,
            [
                *self._ssh_command_prefix,
                self._ssh_binary_file,
                'server',
                '--daemon',
                f'--config-file={config_path}',
                f'--pid-file={pid_path}',
            ],
            iteration.empty_in(),
            iteration.empty_out(),
            iteration.empty_out()
        )():
            raise exception.ServiceError(self._host, 'daemon')

        # wait for server initialization

        for i in range(ping_retry):
            pid = self.get_pid()

            if pid is not None:
                break

            time.sleep(ping_interval)
        else:
            raise exception.ServiceError(self._host, 'pid')

        while not self.ping():
            time.sleep(ping_interval)

            if self.get_pid() is None:
                raise exception.ServiceError(self._host, f'pid_{pid}')

        return pid

    def stop(self, ping_interval=0.1, ping_retry=50):
        assert type(ping_interval) is int or type(ping_interval) is float
        assert type(ping_retry) is int

        pid = self.get_pid()

        if pid is None:
            return

        # kill process

        stderr_list = []

        if connection.run_ssh(
            self._ssh_client,
            [
                'kill',
                '-15',
                str(pid),
            ],
            iteration.empty_in(),
            iteration.empty_out(),
            iteration.collect_out(stderr_list)
        )():
            raise exception.ShellError(
                self._host,
                b''.join(stderr_list).decode()
            )

        for i in range(ping_retry):
            if self.get_pid() is None:
                break

            time.sleep(ping_interval)
        else:
            stderr_list = []

            if connection.run_ssh(
                self._ssh_client,
                [
                    'kill',
                    '-9',
                    str(pid),
                ],
                iteration.empty_in(),
                iteration.empty_out(),
                iteration.collect_out(stderr_list)
            )():
                raise exception.ShellError(
                    self._host,
                    b''.join(stderr_list).decode()
                )

            while self.get_pid() is None:
                time.sleep(ping_interval)

        return pid
