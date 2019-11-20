def make_empty_in():
    yield from ()


def make_empty_out():
    yield


def make_ignore_out():
    while True:
        yield


def make_collect_out(data_list):
    assert type(data_list) is list

    while True:
        data_list.append((yield))
