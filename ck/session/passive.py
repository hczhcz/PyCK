import typing
import urllib.parse

# third-party
import typing_extensions

from ck import exception
from ck import clickhouse
from ck import connection
from ck import iteration


class PassiveSession:
    def __init__(
            self,
            host: str = 'localhost',
            tcp_port: int = 9000,
            http_port: int = 8123,
            ssh_port: int = 22,
            ssh_username: typing.Optional[str] = None,
            ssh_password: typing.Optional[str] = None,
            ssh_public_key: typing.Optional[str] = None,
            ssh_command_prefix: typing.Optional[typing.List[str]] = None,
    ) -> None:
        self._host = host
        self._tcp_port = tcp_port
        self._http_port = http_port
        self._ssh_port = ssh_port
        self._ssh_username = ssh_username
        self._ssh_password = ssh_password
        self._ssh_public_key = ssh_public_key

        if ssh_command_prefix is None:
            self._ssh_command_prefix: typing.List[str] = []
        else:
            self._ssh_command_prefix = ssh_command_prefix

        self._ssh_client = None
        self._ssh_default_data_dir: typing.Optional[str] = None
        self._ssh_binary_file: typing.Optional[str] = None

    def _require_ssh(self) -> None:
        # connect

        if self._ssh_client is None:
            self._ssh_client = connection.connect_ssh(
                self._host,
                self._ssh_port,
                self._ssh_username,
                self._ssh_password,
                self._ssh_public_key
            )

        # lookup

        stdout_list: typing.List[bytes] = []
        stderr_list: typing.List[bytes] = []

        if connection.run_ssh(
                self._ssh_client,
                [
                    *self._ssh_command_prefix,
                    'python3',
                    '-m',
                    'ck.clickhouse.lookup',
                ],
                iteration.empty_in(),
                iteration.collect_out(stdout_list),
                iteration.collect_out(stderr_list)
        )():
            raise exception.ShellError(
                self._host,
                b''.join(stderr_list).decode()
            )

        (
            self._ssh_default_data_dir,
            self._ssh_binary_file,
        ) = b''.join(stdout_list).decode().splitlines()

    def query_async(
            self,
            query_text: str,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            gen_in: typing.Optional[
                typing.Generator[bytes, None, None]
            ] = None,
            gen_out: typing.Optional[
                typing.Generator[None, bytes, None]
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Callable[[], typing.Optional[bytes]]:
        # create connection(s)

        stdout_list: typing.List[bytes] = []
        stderr_list: typing.List[bytes] = []

        if gen_in is None:
            gen_stdin = iteration.given_in([f'{query_text}\n'.encode()])
        else:
            gen_stdin = iteration.concat_in(
                iteration.given_in([f'{query_text}\n'.encode()]),
                gen_in
            )

        if gen_out is None:
            gen_stdout = iteration.collect_out(stdout_list)
        else:
            gen_stdout = gen_out

        gen_stderr = iteration.collect_out(stderr_list)

        if settings is None:
            full_settings: typing.Dict[str, str] = {}
        else:
            full_settings = settings

        if method == 'tcp':
            join_raw = connection.run_process(
                [
                    clickhouse.binary_file(),
                    'client',
                    f'--host={self._host}',
                    f'--port={self._tcp_port}',
                    *(
                        f'--{key}={value}'
                        for key, value in full_settings.items()
                    ),
                ],
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            good_status = 0
        elif method == 'http':
            join_raw = connection.run_http(
                self._host,
                self._http_port,
                f'/?{urllib.parse.urlencode(full_settings)}',
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            good_status = 200
        elif method == 'ssh':
            self._require_ssh()

            assert self._ssh_binary_file is not None

            join_raw = connection.run_ssh(
                self._ssh_client,
                [
                    *self._ssh_command_prefix,
                    self._ssh_binary_file,
                    'client',
                    f'--port={self._tcp_port}',
                    *(
                        f'--{key}={value}'
                        for key, value in full_settings.items()
                    ),
                ],
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            good_status = 0

        # join connection(s)

        def join() -> typing.Optional[bytes]:
            if join_raw() != good_status:
                raise exception.QueryError(
                    self._host,
                    query_text,
                    b''.join(stderr_list).decode()
                )

            if gen_out is None:
                return b''.join(stdout_list)

            return None

        return join

    def query(
            self,
            query_text: str,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            gen_in: typing.Optional[
                typing.Generator[bytes, None, None]
            ] = None,
            gen_out: typing.Optional[
                typing.Generator[None, bytes, None]
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Optional[bytes]:
        return self.query_async(
            query_text,
            method,
            gen_in,
            gen_out,
            settings
        )()

    def ping(
            self,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http'
    ) -> bool:
        try:
            return self.query('select 42', method=method) == b'42\n'
        except ConnectionError:
            return False
        except exception.QueryError:
            return False
