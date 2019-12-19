import io

# third-party
import pandas

import ck
from ck import iteration


def test_session_passive():
    local_session = ck.LocalSession()
    passive_session = ck.PassiveSession()

    local_session.stop()

    for method in 'tcp', 'http', 'ssh':
        assert not passive_session.ping(method=method)

    local_session.start()

    for method in 'tcp', 'http', 'ssh':
        assert passive_session.ping(method=method)

    for method in 'tcp', 'http', 'ssh':
        assert passive_session.query('select 1', method=method) == b'1\n'
        assert passive_session.query_async(
            'select 1',
            method=method
        )() == b'1\n'


def test_session_local():
    local_session = ck.LocalSession()

    pid_1 = local_session.stop()
    assert pid_1 > 0
    assert local_session.get_pid() is None

    pid_2 = local_session.stop()
    assert pid_2 is None
    assert local_session.get_pid() is None

    pid_3 = local_session.start()
    assert pid_3 > 0
    assert local_session.get_pid() > 0

    pid_4 = local_session.start()
    assert pid_4 is None
    assert local_session.get_pid() == pid_3

    for method in 'tcp', 'http', 'ssh':
        assert local_session.query('select 1', method=method) == b'1\n'
        assert local_session.query_async(
            'select 1',
            method=method
        )() == b'1\n'


def test_session_remote():
    remote_session = ck.RemoteSession()

    pid_1 = remote_session.stop()
    assert pid_1 > 0
    assert remote_session.get_pid() is None

    pid_2 = remote_session.stop()
    assert pid_2 is None
    assert remote_session.get_pid() is None

    pid_3 = remote_session.start()
    assert pid_3 > 0
    assert remote_session.get_pid() > 0

    pid_4 = remote_session.start()
    assert pid_4 is None
    assert remote_session.get_pid() == pid_3

    for method in 'tcp', 'http', 'ssh':
        assert remote_session.query('select 1', method=method) == b'1\n'
        assert remote_session.query_async(
            'select 1',
            method=method
        )() == b'1\n'


def test_session_settings():
    local_session = ck.LocalSession()

    query_text = 'select isNull(y) ' \
        'from (select 1 as x) ' \
        'any left join (select 2 as x, 3 as y) ' \
        'using x'

    for method in 'tcp', 'http', 'ssh':
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


def test_session_file():
    local_session = ck.LocalSession()

    local_session.query('drop table if exists pyck_test')
    local_session.query('create table pyck_test (x String) engine = Memory')

    open('/tmp/pyck_test_session_1', 'wb').write(b'hello\nworld\n')
    local_session.query(
        'insert into pyck_test format TSV',
        gen_in=iteration.file_in('/tmp/pyck_test_session_1')
    )
    open('/tmp/pyck_test_session_2', 'wb').write(b'')
    local_session.query(
        'select * from pyck_test format TSV',
        gen_out=iteration.file_out('/tmp/pyck_test_session_2')
    )
    assert open('/tmp/pyck_test_session_2', 'rb').read() == b'hello\nworld\n'

    local_session.query('drop table pyck_test')


def test_session_pandas():
    local_session = ck.LocalSession()

    local_session.query('drop table if exists pyck_test')
    local_session.query('create table pyck_test (x String) engine = Memory')

    dataframe_1 = pandas.DataFrame({'x': pandas.RangeIndex(1000000)})

    read_stream, write_stream = iteration.echo_io()
    join = local_session.query_async(
        'insert into pyck_test format CSVWithNames',
        gen_in=iteration.stream_in(read_stream)
    )
    dataframe_1.to_csv(io.TextIOWrapper(write_stream), index=False)
    join()
    read_stream, write_stream = iteration.echo_io()
    join = local_session.query_async(
        'select * from pyck_test format CSVWithNames',
        gen_out=iteration.stream_out(write_stream)
    )
    dataframe_2 = pandas.read_csv(io.TextIOWrapper(read_stream))
    join()
    assert dataframe_2.x.to_list() == dataframe_1.x.to_list()

    local_session.query('drop table pyck_test')


def test_session_method_tcp_benchmark(benchmark):
    local_session = ck.LocalSession()

    def run():
        local_session.query(
            'select number from numbers(1000000)',
            method='tcp'
        )

    benchmark(run)


def test_session_method_http_benchmark(benchmark):
    local_session = ck.LocalSession()

    def run():
        local_session.query(
            'select number from numbers(1000000)',
            method='http'
        )

    benchmark(run)


def test_session_method_ssh_benchmark(benchmark):
    local_session = ck.LocalSession()

    def run():
        local_session.query(
            'select number from numbers(1000000)',
            method='ssh'
        )

    benchmark(run)
