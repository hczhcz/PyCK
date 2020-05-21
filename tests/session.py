import io
import typing

# third-party
import pandas  # type: ignore[import]
import pytest_benchmark.fixture  # type: ignore[import]
import typing_extensions

import ck
from ck import iteration


METHODS: typing.List[
    typing_extensions.Literal['tcp', 'http', 'ssh']
] = ['tcp', 'http', 'ssh']


def test_session_passive() -> None:
    local_session = ck.LocalSession(start=True)
    passive_session = ck.PassiveSession()

    local_session.stop()

    for method in METHODS:
        assert not passive_session.ping(method=method)

    local_session.start()

    for method in METHODS:
        assert passive_session.ping(method=method)

    for method in METHODS:
        assert passive_session.query('select 1', method=method) == b'1\n'
        assert passive_session.query_async(
            'select 1',
            method=method
        )() == b'1\n'


def test_session_local() -> None:
    local_session = ck.LocalSession(start=True)

    pid_1 = local_session.stop()
    assert pid_1 is not None
    assert local_session.get_pid() is None

    pid_2 = local_session.stop()
    assert pid_2 is None
    assert local_session.get_pid() is None

    pid_3 = local_session.start()
    assert pid_3 is not None
    assert local_session.get_pid() is not None

    pid_4 = local_session.start()
    assert pid_4 is None
    assert local_session.get_pid() == pid_3

    for method in METHODS:
        assert local_session.query('select 1', method=method) == b'1\n'
        assert local_session.query_async(
            'select 1',
            method=method
        )() == b'1\n'


def test_session_remote() -> None:
    remote_session = ck.RemoteSession()

    pid_1 = remote_session.stop()
    assert pid_1 is not None
    assert remote_session.get_pid() is None

    pid_2 = remote_session.stop()
    assert pid_2 is None
    assert remote_session.get_pid() is None

    pid_3 = remote_session.start()
    assert pid_3 is not None
    assert remote_session.get_pid() is not None

    pid_4 = remote_session.start()
    assert pid_4 is None
    assert remote_session.get_pid() == pid_3

    for method in METHODS:
        assert remote_session.query('select 1', method=method) == b'1\n'
        assert remote_session.query_async(
            'select 1',
            method=method
        )() == b'1\n'


def test_session_settings() -> None:
    local_session = ck.LocalSession(start=True)

    query_text = 'select isNull(y) ' \
        'from (select 1 as x) as lhs ' \
        'any left join (select 2 as x, 3 as y) as rhs ' \
        'using x'

    for method in METHODS:
        assert local_session.query(
            query_text,
            method=method,
            settings={'join_use_nulls': '0'}
        ) == b'0\n'
        assert local_session.query(
            query_text,
            method=method,
            settings={'join_use_nulls': '1'}
        ) == b'1\n'


def test_session_gen_bytes() -> None:
    local_session = ck.LocalSession(start=True)

    local_session.query('drop table if exists pyck_test')
    local_session.query('create table pyck_test (x String) engine = Memory')

    local_session.query(
        'insert into pyck_test format TSV',
        data=b'hello\nworld\n'
    )
    assert local_session.query(
        'select * from pyck_test format TSV'
    ) == b'hello\nworld\n'

    local_session.query('drop table pyck_test')


def test_session_gen_stream() -> None:
    local_session = ck.LocalSession(start=True)

    local_session.query('drop table if exists pyck_test')
    local_session.query('create table pyck_test (x Int64) engine = Memory')

    dataframe_1 = pandas.DataFrame({'x': pandas.RangeIndex(1000000)})

    read_stream, write_stream = iteration.echo_io()
    join = local_session.query_stream_async(
        'insert into pyck_test format CSVWithNames',
        stream_in=read_stream
    )
    dataframe_1.to_csv(io.TextIOWrapper(write_stream), index=False)
    join()
    read_stream, write_stream = iteration.echo_io()
    join = local_session.query_stream_async(
        'select * from pyck_test format CSVWithNames',
        stream_out=write_stream
    )
    dataframe_2 = pandas.read_csv(io.TextIOWrapper(read_stream))
    join()
    assert dataframe_2.x.to_list() == dataframe_1.x.to_list()

    local_session.query('drop table pyck_test')


def test_session_gen_file() -> None:
    local_session = ck.LocalSession(start=True)

    local_session.query('drop table if exists pyck_test')
    local_session.query('create table pyck_test (x String) engine = Memory')

    open('/tmp/pyck_test_session_1', 'wb').write(b'hello\nworld\n')
    local_session.query_file(
        'insert into pyck_test format TSV',
        path_in='/tmp/pyck_test_session_1'
    )
    open('/tmp/pyck_test_session_2', 'wb').write(b'')
    local_session.query_file(
        'select * from pyck_test format TSV',
        path_out='/tmp/pyck_test_session_2'
    )
    assert open('/tmp/pyck_test_session_2', 'rb').read() == b'hello\nworld\n'

    local_session.query('drop table pyck_test')


def test_session_gen_pandas() -> None:
    local_session = ck.LocalSession(start=True)

    local_session.query('drop table if exists pyck_test')
    local_session.query('create table pyck_test (x Int64) engine = Memory')

    dataframe_1 = pandas.DataFrame({'x': pandas.RangeIndex(1000000)})

    local_session.query_pandas(
        'insert into pyck_test',
        dataframe=dataframe_1
    )
    dataframe_2 = local_session.query_pandas('select * from pyck_test')
    assert dataframe_2 is not None
    assert dataframe_2.x.to_list() == dataframe_1.x.to_list()

    local_session.query('drop table pyck_test')


def test_session_method_tcp_benchmark(
        benchmark: pytest_benchmark.fixture.BenchmarkFixture
) -> None:
    local_session = ck.LocalSession(start=True)

    def run() -> None:
        local_session.query(
            'select number from numbers(1000000)',
            method='tcp'
        )

    benchmark(run)


def test_session_method_http_benchmark(
        benchmark: pytest_benchmark.fixture.BenchmarkFixture
) -> None:
    local_session = ck.LocalSession(start=True)

    def run() -> None:
        local_session.query(
            'select number from numbers(1000000)',
            method='http'
        )

    benchmark(run)


def test_session_method_ssh_benchmark(
        benchmark: pytest_benchmark.fixture.BenchmarkFixture
) -> None:
    local_session = ck.LocalSession(start=True)

    def run() -> None:
        local_session.query(
            'select number from numbers(1000000)',
            method='ssh'
        )

    benchmark(run)
