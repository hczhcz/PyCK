import typing


def empty_in() -> typing.Generator[bytes, None, None]:
    yield from ()


def given_in(
        data_list: typing.List[bytes]
) -> typing.Generator[bytes, None, None]:
    yield from data_list


def concat_in(
        gen_1: typing.Generator[bytes, None, None],
        gen_2: typing.Generator[bytes, None, None]
) -> typing.Generator[bytes, None, None]:
    yield from gen_1
    yield from gen_2


def empty_out() -> typing.Generator[None, bytes, None]:
    data = yield

    if data:
        raise RuntimeError()

    yield


def ignore_out() -> typing.Generator[None, bytes, None]:
    data = yield

    while data:
        data = yield

    yield


def collect_out(
        data_list: typing.List[bytes]
) -> typing.Generator[None, bytes, None]:
    data = yield

    while data:
        data_list.append(data)

        data = yield

    yield
