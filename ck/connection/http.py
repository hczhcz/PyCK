import http.client
import threading
import typing


def run_http(
        host: str,
        port: int,
        path: str,
        gen_stdin: typing.Generator[bytes, None, None],
        gen_stdout: typing.Generator[None, bytes, None],
        gen_stderr: typing.Generator[None, bytes, None],
        buffer_size: int = 1 << 20,
        join_interval: float = 0.1
) -> typing.Callable[[], int]:
    connection = None
    response = None
    error = None

    # create thread

    def post_request() -> None:
        nonlocal connection
        nonlocal response
        nonlocal error

        try:
            connection = http.client.HTTPConnection(host, port)
            connection.request('POST', path, gen_stdin)

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
        except Exception as raw_error:
            error = raw_error

    thread = threading.Thread(target=post_request)

    thread.start()

    # join thread

    def join() -> int:
        while error is None and thread.is_alive():
            thread.join(join_interval)

        if error is not None:
            if connection is not None:
                connection.close()

            raise error

        assert response

        return response.status

    return join
