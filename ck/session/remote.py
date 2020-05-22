import pathlib
import time
import typing

from ck import connection
from ck import exception
from ck import iteration
from ck.session import passive


class RemoteSession(passive.PassiveSession):
    def __init__(
            self,
            host: str = 'localhost',
            tcp_port: int = 9000,
            http_port: int = 8123,
            user: str = 'default',
            password: str = '',
            default_settings: typing.Optional[typing.Dict[str, str]] = None,
            ssh_port: int = 22,
            ssh_username: typing.Optional[str] = None,
            ssh_password: typing.Optional[str] = None,
            ssh_public_key: typing.Optional[str] = None,
            ssh_command_prefix: typing.Optional[typing.List[str]] = None,
            data_dir: typing.Optional[str] = None,
            config: typing.Optional[typing.Dict[str, typing.Any]] = None,
            auto_start: bool = True,
            stop: bool = False,
            start: bool = False
    ) -> None:
        super().__init__(
            host,
            tcp_port,
            http_port,
            user,
            password,
            default_settings,
            ssh_port,
            ssh_username,
            ssh_password,
            ssh_public_key,
            ssh_command_prefix
        )

        self._require_ssh()

        if data_dir is None:
            assert self._ssh_default_data_dir is not None

            self._path = pathlib.Path(self._ssh_default_data_dir)
        else:
            self._path = pathlib.Path(data_dir)

        self._config = config or {}
        self._auto_start = auto_start

        if stop:
            self.stop()

        if start:
            self.start()

    def _prepare(self) -> None:
        if self._auto_start:
            self.start()

    def get_pid(self) -> typing.Optional[int]:
        pid_path = self._path.joinpath('pid')

        # get pid

        stdout_list: typing.List[bytes] = []

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
            return None

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
            return None

        return pid

    def start(
            self,
            ping_interval: float = 0.1,
            ping_retry: int = 50
    ) -> typing.Optional[int]:
        pid = self.get_pid()

        if pid is not None:
            return None

        config_path = self._path.joinpath('config.xml')
        pid_path = self._path.joinpath('pid')

        # create dir

        stderr_list: typing.List[bytes] = []

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
                iteration.given_in([repr({
                    'tcp_port': self._tcp_port,
                    'http_port': self._http_port,
                    'user': self._user,
                    'password': self._password,
                    'data_dir': str(self._path),
                    'config': self._config,
                }).encode()]),
                iteration.empty_out(),
                iteration.collect_out(stderr_list)
        )():
            raise exception.ShellError(
                self._host,
                b''.join(stderr_list).decode()
            )

        # run

        assert self._ssh_binary_file is not None

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

        for _ in range(ping_retry):
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

    def stop(
            self,
            ping_interval: float = 0.1,
            ping_retry: int = 50
    ) -> typing.Optional[int]:
        pid = self.get_pid()

        if pid is None:
            return None

        # kill process

        stderr_list: typing.List[bytes] = []

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

        for _ in range(ping_retry):
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

            while self.get_pid() is not None:
                time.sleep(ping_interval)

        return pid
