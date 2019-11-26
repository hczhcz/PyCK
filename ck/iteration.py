import types


def make_empty_in():
    yield from ()


def make_given_in(data):
    assert type(data) is bytes

    yield data


def make_empty_out():
    yield


def make_ignore_out():
    while True:
        yield


def make_collect_out(data_list):
    assert data_list == []

    while True:
        data_list.append((yield))


def make_concat(gen_1, gen_2):
    assert type(gen_1) is types.GeneratorType
    assert type(gen_2) is types.GeneratorType

    yield from gen_1
    yield from gen_2
