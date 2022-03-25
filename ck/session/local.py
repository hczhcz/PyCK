import os
import pathlib
import time
import typing

# third-party
import typing_extensions

from ck import clickhouse
from ck import connection
from ck import exception
from ck import iteration
from ck.session import passive


class LocalSession(passive.PassiveSession):
    def __init__(
            self,
            host: str = 'localhost',
            tcp_port: int = 9000,
            http_port: int = 8123,
            user: str = 'default',
            password: str = '',
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None,
            http_session: bool = False,
            ssh_port: int = 22,
            ssh_username: typing.Optional[str] = None,
            ssh_password: typing.Optional[str] = None,
            ssh_public_key: typing.Optional[str] = None,
            ssh_command_prefix: typing.Optional[typing.List[str]] = None,
            data_dir: typing.Optional[str] = None,
            memory_limit: typing.Optional[int] = None,
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
            method,
            settings,
            http_session,
            ssh_port,
            ssh_username,
            ssh_password,
            ssh_public_key,
            ssh_command_prefix
        )

        if data_dir is None:
            self._path = pathlib.Path(clickhouse.default_data_dir())
        else:
            self._path = pathlib.Path(data_dir)

        self._memory_limit = memory_limit or 0
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

        try:
            pid_text, = pid_path.open().read().splitlines()
        except FileNotFoundError:
            return None

        pid = int(pid_text)

        # find process

        try:
            os.kill(pid, 0)
        except OSError:
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

        self._path.mkdir(parents=True, exist_ok=True)

        # setup

        clickhouse.create_config(
            self._tcp_port,
            self._http_port,
            self._user,
            self._password,
            str(self._path),
            self._memory_limit,
            self._config
        )

        # run

        if connection.run_process(
                [
                    clickhouse.binary_file(),
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

        os.kill(pid, 15)

        for _ in range(ping_retry):
            if self.get_pid() is None:
                break

            time.sleep(ping_interval)
        else:
            os.kill(pid, 9)

            while self.get_pid() is not None:
                time.sleep(ping_interval)

        return pid
