import threading
import typing
import urllib.parse
import uuid

# third-party
import numpy
import pandas  # type: ignore[import]
import paramiko
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
            user: str = 'default',
            password: str = '',
            method: typing_extensions.Literal['tcp', 'http', 'ssh'] = 'http',
            settings: typing.Optional[typing.Dict[str, str]] = None,
            ssh_port: int = 22,
            ssh_username: typing.Optional[str] = None,
            ssh_password: typing.Optional[str] = None,
            ssh_public_key: typing.Optional[str] = None,
            ssh_command_prefix: typing.Optional[typing.List[str]] = None
    ) -> None:
        self._host = host
        self._tcp_port = tcp_port
        self._http_port = http_port
        self._user = user
        self._password = password
        self._method = method
        self._settings = settings or {}
        self._ssh_port = ssh_port
        self._ssh_username = ssh_username
        self._ssh_password = ssh_password
        self._ssh_public_key = ssh_public_key
        self._ssh_command_prefix = ssh_command_prefix or []

        self._session_id = str(uuid.uuid4())
        self._ssh_client: typing.Optional[paramiko.SSHClient] = None
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
                b''.join(stderr_list)
            )

        (
            self._ssh_default_data_dir,
            self._ssh_binary_file,
        ) = b''.join(stdout_list).decode().splitlines()

    def _prepare(self) -> None:
        pass

    def _run(
            self,
            query: str,
            gen_in: typing.Generator[bytes, None, None],
            gen_out: typing.Generator[None, bytes, None],
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ],
            settings: typing.Optional[typing.Dict[str, str]]
    ) -> typing.Callable[[], None]:
        self._prepare()

        # create connection(s)

        stderr_list: typing.List[bytes] = []

        gen_stdin = iteration.concat_in(
            iteration.given_in([f'{query}\n'.encode()]),
            gen_in
        )
        gen_stdout = gen_out
        gen_stderr = iteration.collect_out(stderr_list)

        real_method = method or self._method
        real_settings = {
            **(
                {
                    'session_id': self._session_id,
                }
                if real_method == 'http'
                else {}
            ),
            **self._settings,
            **(settings or {}),
        }

        if real_method == 'tcp':
            raw_join = connection.run_process(
                [
                    clickhouse.binary_file(),
                    'client',
                    f'--host={self._host}',
                    f'--port={self._tcp_port}',
                    f'--user={self._user}',
                    *(
                        [f'--password={self._password}']
                        if self._password
                        else []
                    ),
                    *(
                        f'--{key}={value}'
                        for key, value in real_settings.items()
                    ),
                ],
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            good_status = 0
        elif real_method == 'http':
            raw_join = connection.run_http(
                self._host,
                self._http_port,
                f'/?{urllib.parse.urlencode(real_settings)}',
                {
                    'X-ClickHouse-User': self._user,
                    **(
                        {
                            'X-ClickHouse-Key': self._password,
                        }
                        if self._password
                        else {}
                    ),
                },
                gen_stdin,
                gen_stdout,
                gen_stderr
            )
            good_status = 200
        elif real_method == 'ssh':
            self._require_ssh()

            assert self._ssh_client is not None
            assert self._ssh_binary_file is not None

            raw_join = connection.run_ssh(
                self._ssh_client,
                [
                    *self._ssh_command_prefix,
                    self._ssh_binary_file,
                    'client',
                    f'--port={self._tcp_port}',
                    f'--user={self._user}',
                    *(
                        [f'--password={self._password}']
                        if self._password
                        else []
                    ),
                    *(
                        f'--{key}={value}'
                        for key, value in real_settings.items()
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
                    query,
                    b''.join(stderr_list)
                )

        return join

    def query_async(
            self,
            query: str,
            data: bytes = b'',
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Callable[[], bytes]:
        stdout_list: typing.List[bytes] = []

        gen_in = iteration.given_in([data])
        gen_out = iteration.collect_out(stdout_list)

        raw_join = self._run(query, gen_in, gen_out, method, settings)

        def join() -> bytes:
            raw_join()

            return b''.join(stdout_list)

        return join

    def query(
            self,
            query: str,
            data: bytes = b'',
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> bytes:
        return self.query_async(query, data, method, settings)()

    def query_stream_async(
            self,
            query: str,
            stream_in: typing.Optional[typing.BinaryIO] = None,
            stream_out: typing.Optional[typing.BinaryIO] = None,
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
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

        return self._run(query, gen_in, gen_out, method, settings)

    def query_stream(
            self,
            query: str,
            stream_in: typing.Optional[typing.BinaryIO] = None,
            stream_out: typing.Optional[typing.BinaryIO] = None,
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> None:
        self.query_stream_async(
            query,
            stream_in,
            stream_out,
            method,
            settings
        )()

    def query_pipe_async(
            self,
            query: str,
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> typing.Callable[[], None]:
        gen_in = iteration.pipe_in()
        gen_out = iteration.pipe_out()

        return self._run(query, gen_in, gen_out, method, settings)

    def query_pipe(
            self,
            query: str,
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> None:
        self.query_pipe_async(
            query,
            method,
            settings
        )()

    def query_file_async(
            self,
            query: str,
            path_in: typing.Optional[str] = None,
            path_out: typing.Optional[str] = None,
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
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

        return self._run(query, gen_in, gen_out, method, settings)

    def query_file(
            self,
            query: str,
            path_in: typing.Optional[str] = None,
            path_out: typing.Optional[str] = None,
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None
    ) -> None:
        self.query_file_async(
            query,
            path_in,
            path_out,
            method,
            settings
        )()

    def query_pandas_async(
            self,
            query: str,
            dataframe: typing.Optional[pandas.DataFrame] = None,
            encoding: typing.Optional[str] = 'utf-8',
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
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
            f'{query} format ArrowStream',
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

                    if encoding is not None:
                        def decode(
                                value: typing.Any
                        ) -> typing.Any:
                            if type(value) is bytes:
                                assert encoding is not None

                                return value.decode(encoding)

                            if type(value) is bytearray:
                                assert encoding is not None

                                return value.decode(encoding)

                            if type(value) is tuple:
                                return tuple(
                                    decode(child)
                                    for child in value
                                )

                            if type(value) is list:
                                return [
                                    decode(child)
                                    for child in value
                                ]

                            if type(value) is numpy.ndarray:
                                return numpy.array([
                                    decode(child)
                                    for child in value
                                ])

                            if type(value) is set:
                                return {
                                    decode(child)
                                    for child in value
                                }

                            if type(value) is frozenset:
                                return frozenset(
                                    decode(child)
                                    for child in value
                                )

                            if type(value) is dict:
                                return {
                                    key: decode(child)
                                    for key, child in value.items()
                                }

                            return value

                        dataframe = pandas.DataFrame({
                            column: (
                                dataframe[column].apply(decode)
                                if dataframe[column].dtype == 'O'
                                else dataframe[column]
                            )
                            for column in dataframe
                        })
                else:
                    if encoding is not None:
                        def encode(
                                value: typing.Any
                        ) -> typing.Any:
                            if type(value) is str:
                                assert encoding is not None

                                return value.encode(encoding)

                            if type(value) is tuple:
                                return tuple(
                                    encode(child)
                                    for child in value
                                )

                            if type(value) is list:
                                return [
                                    encode(child)
                                    for child in value
                                ]

                            if type(value) is numpy.ndarray:
                                return numpy.array([
                                    encode(child)
                                    for child in value
                                ])

                            if type(value) is set:
                                return {
                                    encode(child)
                                    for child in value
                                }

                            if type(value) is frozenset:
                                return frozenset(
                                    encode(child)
                                    for child in value
                                )

                            if type(value) is dict:
                                return {
                                    key: encode(child)
                                    for key, child in value.items()
                                }

                            return value

                        dataframe = pandas.DataFrame({
                            column: (
                                dataframe[column].apply(encode)
                                if dataframe[column].dtype == 'O'
                                else dataframe[column]
                            )
                            for column in dataframe
                        })

                    table = pyarrow.Table.from_arrays([
                        pyarrow.array(dataframe[column].values)
                        for column in dataframe
                    ], dataframe.columns)
                    batch = pyarrow.RecordBatchStreamWriter(
                        write_stream,
                        table.schema
                    )
                    batch.write_table(table)
                    dataframe = None
                    batch.close()
                    write_stream.close()
            except pyarrow.ArrowInvalid:
                pass
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
            query: str,
            dataframe: typing.Optional[pandas.DataFrame] = None,
            encoding: typing.Optional[str] = 'utf-8',
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None,
            settings: typing.Optional[typing.Dict[str, str]] = None,
            join_interval: float = 0.1
    ) -> typing.Optional[pandas.DataFrame]:
        return self.query_pandas_async(
            query,
            dataframe,
            encoding,
            method,
            settings,
            join_interval
        )()

    def ping(
            self,
            method: typing.Optional[
                typing_extensions.Literal['tcp', 'http', 'ssh']
            ] = None
    ) -> bool:
        try:
            return self.query('select 42', method=method) == b'42\n'
        except ConnectionError:
            return False
        except OSError:
            return False
        except exception.QueryError:
            return False
