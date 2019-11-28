import io


def stream_in(stream, buffer_size=1 << 20):
    assert type(stream) is io.BufferedReader
    assert type(buffer_size) is int

    data = stream.read(buffer_size)

    while data:
        yield data

        data = stream.read(buffer_size)


def stream_out(stream):
    assert type(stream) is io.BufferedWriter

    while True:
        stream.write((yield))


def file_in(path, buffer_size=1 << 20):
    assert type(path) is str
    assert type(buffer_size) is int

    yield from stream_in(open(path, 'rb'), buffer_size)


def file_out(path):
    assert type(path) is str

    yield from stream_out(open(path, 'wb'))
