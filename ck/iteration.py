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
