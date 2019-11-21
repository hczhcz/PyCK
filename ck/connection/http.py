import http.client
import threading
import types


def run(
    host,
    port,
    path,
    gen_stdin,
    gen_stdout,
    gen_stderr,
    buffer_size=1 << 20,
    join_interval=0.1
):
    assert type(host) is str
    assert type(port) is int
    assert type(path) is str
    assert type(gen_stdin) is types.GeneratorType
    assert type(gen_stdout) is types.GeneratorType
    assert type(gen_stderr) is types.GeneratorType
    assert type(buffer_size) is int
    assert type(join_interval) is int or type(join_interval) is float

    connection = None
    response = None
    error = None

    # create thread

    def make_stdin():
        for data in gen_stdin:
            assert type(data) is bytes

            yield data

    def post_request():
        nonlocal connection
        nonlocal response
        nonlocal error

        try:
            connection = http.client.HTTPConnection(host, port)
            connection.request('POST', path, make_stdin())

            response = connection.getresponse()

            if response.status == 200:
                gen_out = gen_stdout
            else:
                gen_out = gen_stderr

            next(gen_out)
            data = response.read(buffer_size)

            while data:
                gen_out.send(data)
                data = response.read(buffer_size)
        except Exception as e:
            error = e

    thread = threading.Thread(target=post_request)

    thread.start()

    # join thread

    def join():
        while error is None and thread.is_alive():
            thread.join(join_interval)

        if error is not None:
            if connection is not None:
                connection.close()

            raise error

        return response.status

    return join
