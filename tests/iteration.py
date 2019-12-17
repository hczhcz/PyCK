from ck import iteration


def test_adhoc_in():
    gen_in = iteration.empty_in()
    assert list(gen_in) == []

    gen_in = iteration.given_in([b'1', b'2', b'3'])
    assert list(gen_in) == [b'1', b'2', b'3']

    gen_in = iteration.concat_in(
        iteration.given_in([b'1', b'2']),
        iteration.given_in([b'3'])
    )
    assert list(gen_in) == [b'1', b'2', b'3']


def test_adhoc_out():
    gen_out = iteration.empty_out()
    next(gen_out)
    ok = False
    try:
        gen_out.send(b'1')
    except StopIteration:
        ok = True
    assert ok

    gen_out = iteration.ignore_out()
    next(gen_out)
    gen_out.send(b'1')
    gen_out.send(b'2')
    gen_out.send(b'3')

    stdout_list = []
    gen_out = iteration.collect_out(stdout_list)
    next(gen_out)
    gen_out.send(b'1')
    gen_out.send(b'2')
    gen_out.send(b'3')
    assert stdout_list == [b'1', b'2', b'3']


def test_io_in():
    open('/tmp/pyck_test', 'wb').write(b'hello\n')
    gen_in = iteration.stream_in(open('/tmp/pyck_test', 'rb'))
    assert list(gen_in) == [b'hello\n']

    open('/tmp/pyck_test', 'wb').write(b'hello\n')
    gen_in = iteration.file_in('/tmp/pyck_test')
    assert list(gen_in) == [b'hello\n']


def test_io_out():
    gen_out = iteration.stream_out(open('/tmp/pyck_test', 'wb'))
    next(gen_out)
    gen_out.send(b'world\n')
    assert open('/tmp/pyck_test', 'rb').read() == b'world\n'

    gen_out = iteration.file_out('/tmp/pyck_test')
    next(gen_out)
    gen_out.send(b'world\n')
    assert open('/tmp/pyck_test', 'rb').read() == b'world\n'
