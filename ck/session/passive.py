import threading
import typing
import urllib.parse

# third-party
import pandas  # type: ignore[import]
import pyarrow  # type: ignore[import]
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

    def _prepare(self) -> None:
        pass

    def _run(
            self,
            query_text: str,
            gen_in: typing.Generator[bytes, None, None],
            gen_out: typing.Generator[None, bytes, None],
            method: typing_extensions.Literal['tcp', 'http', 'ssh'],
            settings: typing.Optional[typing.Dict[str, str]]
    ) -> typing.Callable[[], None]:
        self._prepare()

        # create connection(s)

        stderr_list: typing.List[bytes] = []

        gen_stdin = iteration.concat_in(
            iteration.given_in([f'{query_text}\n'.encode()]),
            gen_in
        )
        gen_stdout = gen_out
        gen_stderr = iteration.collect_out(stderr_list)

        if settings is None:
            full_settings: typing.Dict[str, str] = {}
        else:
            full_settings = settings

        if method == 'tcp':
            raw_join = connection.run_process(
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
            raw_join = connection.run_http(
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

            raw_join = connection.run_ssh(
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

        def join() -> None:
            if raw_join() != good_status:
                raise exception.QueryError(
                    self._host,
                    query_text,
                    b''.join(stderr_list).decode()
                )

        return join

    def query_async(
            self,
            query_text: str,
            data: bytes = b'',
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Callable[[], bytes]:
        stdout_list: typing.List[bytes] = []

        gen_in = iteration.given_in([data])
        gen_out = iteration.collect_out(stdout_list)

        raw_join = self._run(query_text, gen_in, gen_out, method, settings)

        def join() -> bytes:
            raw_join()

            return b''.join(stdout_list)

        return join

    def query(
            self,
            query_text: str,
            data: bytes = b'',
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> bytes:
        return self.query_async(query_text, data, method, settings)()

    def query_stream_async(
            self,
            query_text: str,
            stream_in: typing.Optional[typing.BinaryIO] = None,
            stream_out: typing.Optional[typing.BinaryIO] = None,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Callable[[], None]:
        if stream_in is None:
            gen_in = iteration.empty_in()
        else:
            gen_in = iteration.stream_in(stream_in)

        if stream_out is None:
            gen_out = iteration.empty_out()
        else:
            gen_out = iteration.stream_out(stream_out)

        return self._run(query_text, gen_in, gen_out, method, settings)

    def query_stream(
            self,
            query_text: str,
            stream_in: typing.Optional[typing.BinaryIO] = None,
            stream_out: typing.Optional[typing.BinaryIO] = None,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> None:
        self.query_stream_async(
            query_text,
            stream_in,
            stream_out,
            method,
            settings
        )()

    def query_file_async(
            self,
            query_text: str,
            path_in: typing.Optional[str] = None,
            path_out: typing.Optional[str] = None,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Callable[[], None]:
        if path_in is None:
            gen_in = iteration.empty_in()
        else:
            gen_in = iteration.file_in(path_in)

        if path_out is None:
            gen_out = iteration.empty_out()
        else:
            gen_out = iteration.file_out(path_out)

        return self._run(query_text, gen_in, gen_out, method, settings)

    def query_file(
            self,
            query_text: str,
            path_in: typing.Optional[str] = None,
            path_out: typing.Optional[str] = None,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> None:
        self.query_file_async(
            query_text,
            path_in,
            path_out,
            method,
            settings
        )()

    def query_pandas_async(
            self,
            query_text: str,
            dataframe: typing.Optional[pandas.DataFrame] = None,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None,
            join_interval: float = 0.1
    ) -> typing.Callable[[], typing.Optional[pandas.DataFrame]]:
        batch = None
        error = None

        # prepare

        read_stream, write_stream = iteration.echo_io()

        if dataframe is None:
            gen_in = iteration.empty_in()
            gen_out = iteration.stream_out(write_stream)
        else:
            gen_in = iteration.stream_in(read_stream)
            gen_out = iteration.empty_out()

        raw_join = self._run(
            f'{query_text} format ArrowStream',
            gen_in,
            gen_out,
            method,
            settings
        )

        # create thread

        def handle_batch() -> None:
            nonlocal dataframe
            nonlocal batch
            nonlocal error

            try:
                if dataframe is None:
                    batch = pyarrow.RecordBatchStreamReader(read_stream)
                    dataframe = batch.read_pandas()
                else:
                    table = pyarrow.Table.from_pandas(dataframe)
                    batch = pyarrow.RecordBatchStreamWriter(
                        write_stream,
                        table.schema
                    )
                    batch.write_table(table)
                    dataframe = None
                    batch.close()
                    write_stream.close()

            except BaseException as raw_error:  # pylint: disable=broad-except
                error = raw_error

        thread = threading.Thread(target=handle_batch)

        thread.start()

        # join thread

        def join() -> typing.Optional[pandas.DataFrame]:
            while error is None and thread.is_alive():
                thread.join(join_interval)

            if error is not None:
                raise error  # pylint: disable=raising-bad-type

            raw_join()

            return dataframe

        return join

    def query_pandas(
            self,
            query_text: str,
            dataframe: typing.Optional[pandas.DataFrame] = None,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None,
            join_interval: float = 0.1
    ) -> typing.Optional[pandas.DataFrame]:
        return self.query_pandas_async(
            query_text,
            dataframe,
            method,
            settings,
            join_interval
        )()

    def ping(
            self,
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http'
    ) -> bool:
        try:
            return self.query('select 42', method=method) == b'42\n'
        except ConnectionError:
            return False
        except OSError:
            return False
        except exception.QueryError:
            return False
