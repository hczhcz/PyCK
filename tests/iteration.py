import typing

from ck import iteration


def test_iteration_empty_in() -> None:
    gen_in = iteration.empty_in()

    assert list(gen_in) == []


def test_iteration_given_in() -> None:
    gen_in = iteration.given_in([b'1', b'2', b'3'])

    assert list(gen_in) == [b'1', b'2', b'3']


def test_iteration_concat_in() -> None:
    gen_in = iteration.concat_in(
        iteration.given_in([b'1', b'2']),
        iteration.given_in([b'3'])
    )

    assert list(gen_in) == [b'1', b'2', b'3']


def test_iteration_empty_out() -> None:
    gen_out = iteration.empty_out()
    next(gen_out)
    gen_out.send(b'')

    assert not list(gen_out)

    gen_out = iteration.empty_out()
    next(gen_out)
    catched_error = False

    try:
        gen_out.send(b'1')
    except RuntimeError:
        catched_error = True

    assert catched_error


def test_iteration_ignore_out() -> None:
    gen_out = iteration.ignore_out()
    next(gen_out)
    gen_out.send(b'1')
    gen_out.send(b'2')
    gen_out.send(b'3')
    gen_out.send(b'')

    assert not list(gen_out)


def test_iteration_collect_out() -> None:
    data_list: typing.List[bytes] = []
    gen_out = iteration.collect_out(data_list)
    next(gen_out)
    gen_out.send(b'1')
    gen_out.send(b'2')
    gen_out.send(b'3')
    gen_out.send(b'')

    assert data_list == [b'1', b'2', b'3']
    assert not list(gen_out)


def test_iteration_stream_in() -> None:
    open('/tmp/pyck_test_iteration_1', 'wb').write(b'hello\n')
    gen_in = iteration.stream_in(open('/tmp/pyck_test_iteration_1', 'rb'))

    assert list(gen_in) == [b'hello\n']


# TODO: test pipe_in


def test_iteration_file_in() -> None:
    open('/tmp/pyck_test_iteration_2', 'wb').write(b'hello\n')
    gen_in = iteration.file_in('/tmp/pyck_test_iteration_2')

    assert list(gen_in) == [b'hello\n']


def test_iteration_stream_out() -> None:
    gen_out = iteration.stream_out(open('/tmp/pyck_test_iteration_3', 'wb'))
    next(gen_out)
    gen_out.send(b'world\n')
    gen_out.send(b'')

    assert open('/tmp/pyck_test_iteration_3', 'rb').read() == b'world\n'


# TODO: test pipe_out


def test_iteration_file_out() -> None:
    gen_out = iteration.file_out('/tmp/pyck_test_iteration_4')
    next(gen_out)
    gen_out.send(b'world\n')
    gen_out.send(b'')

    assert open('/tmp/pyck_test_iteration_4', 'rb').read() == b'world\n'


def test_iteration_echo_io() -> None:
    read_stream, write_stream = iteration.echo_io()

    write_stream.write(b'hello\n')
    data = read_stream.read(6)

    assert data == b'hello\n'

    write_stream.write(b'world\n')
    data = read_stream.read(2)

    assert data == b'wo'

    data = read_stream.read(2)

    assert data == b'rl'

    write_stream.close()
    data = read_stream.read()

    assert data == b'd\n'

    data = read_stream.read()

    assert data == b''
