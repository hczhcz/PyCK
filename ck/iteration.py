import io
import types


def empty_in():
    yield from ()


def given_in(data):
    assert type(data) is bytes

    yield data


def stream_in(stream, buffer_size=1 << 20):
    assert type(stream) is io.BufferedReader
    assert type(buffer_size) is int

    data = stream.read(buffer_size)

    while data:
        yield data

        data = stream.read(buffer_size)


def empty_out():
    yield


def ignore_out():
    while True:
        yield


def collect_out(data_list):
    assert data_list == []

    while True:
        data_list.append((yield))


def stream_out(stream):
    assert type(stream) is io.BufferedWriter

    while True:
        stream.write((yield))


def concat(gen_1, gen_2):
    assert type(gen_1) is types.GeneratorType
    assert type(gen_2) is types.GeneratorType

    yield from gen_1
    yield from gen_2
