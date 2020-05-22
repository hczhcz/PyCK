import typing

# third-party
import pytest_benchmark.fixture  # type: ignore[import]

import ck
from ck import clickhouse
from ck import connection
from ck import iteration


def test_connection_process() -> None:
    ck.LocalSession(start=True)

    stdout_list: typing.List[bytes] = []
    status = connection.run_process(
        [clickhouse.binary_file(), 'client'],
        iteration.given_in([b'select 1']),
        iteration.collect_out(stdout_list),
        iteration.empty_out()
    )()

    assert stdout_list == [b'1\n']
    assert status == 0


def test_connection_http() -> None:
    ck.LocalSession(start=True)

    stdout_list: typing.List[bytes] = []
    status = connection.run_http(
        'localhost',
        8123,
        '/',
        {},
        iteration.given_in([b'select 1']),
        iteration.collect_out(stdout_list),
        iteration.empty_out()
    )()

    assert stdout_list == [b'1\n']
    assert status == 200


def test_connection_ssh() -> None:
    ck.LocalSession(start=True)

    ssh_client = connection.connect_ssh('localhost', 22)

    stdout_list: typing.List[bytes] = []
    status = connection.run_ssh(
        ssh_client,
        [clickhouse.binary_file(), 'client'],
        iteration.given_in([b'select 1']),
        iteration.collect_out(stdout_list),
        iteration.empty_out()
    )()

    assert stdout_list == [b'1\n']
    assert status == 0


def test_connection_process_benchmark(
        benchmark: pytest_benchmark.fixture.BenchmarkFixture
) -> None:
    ck.LocalSession(start=True)

    def run() -> None:
        connection.run_process(
            [clickhouse.binary_file(), 'client'],
            iteration.given_in([b'select number from numbers(1000000)']),
            iteration.ignore_out(),
            iteration.empty_out()
        )()

    benchmark(run)


def test_connection_http_benchmark(
        benchmark: pytest_benchmark.fixture.BenchmarkFixture
) -> None:
    ck.LocalSession(start=True)

    def run() -> None:
        connection.run_http(
            'localhost',
            8123,
            '/',
            {},
            iteration.given_in([b'select number from numbers(1000000)']),
            iteration.ignore_out(),
            iteration.empty_out()
        )()

    benchmark(run)


def test_connection_ssh_benchmark(
        benchmark: pytest_benchmark.fixture.BenchmarkFixture
) -> None:
    ck.LocalSession(start=True)

    ssh_client = connection.connect_ssh('localhost', 22)

    def run() -> None:
        connection.run_ssh(
            ssh_client,
            [clickhouse.binary_file(), 'client'],
            iteration.given_in([b'select number from numbers(1000000)']),
            iteration.ignore_out(),
            iteration.empty_out()
        )()

    benchmark(run)
