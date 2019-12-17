import ck
from ck import connection
from ck import iteration


def test_connection_process():
    ck.LocalSession()

    stdout_list = []
    status = connection.run_process(
        ['clickhouse', 'client'],
        iteration.given_in([b'select 1']),
        iteration.collect_out(stdout_list),
        iteration.empty_out()
    )()

    assert stdout_list == [b'1\n']
    assert status == 0


def test_connection_http():
    ck.LocalSession()

    stdout_list = []
    status = connection.run_http(
        'localhost',
        8123,
        '/',
        iteration.given_in([b'select 1']),
        iteration.collect_out(stdout_list),
        iteration.empty_out()
    )()

    assert stdout_list == [b'1\n']
    assert status == 200


def test_connection_ssh():
    ck.LocalSession()

    ssh_client = connection.connect_ssh('localhost')

    stdout_list = []
    status = connection.run_ssh(
        ssh_client,
        ['clickhouse', 'client'],
        iteration.given_in([b'select 1']),
        iteration.collect_out(stdout_list),
        iteration.empty_out()
    )()

    assert stdout_list == [b'1\n']
    assert status == 0


def test_connection_process_benchmark(benchmark):
    ck.LocalSession()

    def run():
        connection.run_process(
            ['clickhouse', 'client'],
            iteration.given_in([b'select number from numbers(1000000)']),
            iteration.ignore_out(),
            iteration.empty_out()
        )()

    benchmark(run)


def test_connection_http_benchmark(benchmark):
    ck.LocalSession()

    def run():
        connection.run_http(
            'localhost',
            8123,
            '/',
            iteration.given_in([b'select number from numbers(1000000)']),
            iteration.ignore_out(),
            iteration.empty_out()
        )()

    benchmark(run)


def test_connection_ssh_benchmark(benchmark):
    ck.LocalSession()

    ssh_client = connection.connect_ssh('localhost')

    def run():
        connection.run_ssh(
            ssh_client,
            ['clickhouse', 'client'],
            iteration.given_in([b'select number from numbers(1000000)']),
            iteration.ignore_out(),
            iteration.empty_out()
        )()

    benchmark(run)
