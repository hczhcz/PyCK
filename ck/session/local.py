import os
import pathlib
import time

from ck import exception
from ck import iteration
from ck.clickhouse import lookup
from ck.clickhouse import setup
from ck.connection import process
from ck.session import passive


class LocalSession(passive.PassiveSession):
    def __init__(
        self,
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
            'localhost',
            tcp_port,
            http_port,
            ssh_port,
            ssh_username,
            ssh_password,
            ssh_public_key,
            ssh_command_prefix
        )

        if data_dir is None:
            self._path = pathlib.Path(lookup.default_data_dir())
        else:
            self._path = pathlib.Path(data_dir)

        self._config = config

        if stop:
            self.stop()

        if start:
            self.start()

    def get_pid(
        self
    ):
        pid_path = self._path.joinpath('pid')

        try:
            pid_text, = pid_path.open().read().splitlines()
        except FileNotFoundError:
            return

        pid = int(pid_text)

        try:
            os.kill(pid, 0)
        except OSError:
            return

        return pid

    def start(
        self,
        ping_interval=0.1,
        ping_retry=50
    ):
        assert type(ping_interval) is int or type(ping_interval) is float
        assert type(ping_retry) is int

        pid = self.get_pid()

        if pid is not None:
            return

        config_path = self._path.joinpath('config.xml')
        pid_path = self._path.joinpath('pid')

        # create dir

        self._path.mkdir(parents=True, exist_ok=True)

        # setup

        setup.create_config(
            self._tcp_port,
            self._http_port,
            str(self._path),
            self._config
        )

        # run

        if process.run(
            [
                lookup.binary_file(),
                'server',
                '--daemon',
                f'--config-file={config_path}',
                f'--pid-file={pid_path}',
            ],
            iteration.make_empty_in(),
            iteration.make_empty_out(),
            iteration.make_empty_out()
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

    def stop(
        self,
        ping_interval=0.1,
        ping_retry=50
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
