import shlex
import threading
import typing

# third-party
import paramiko  # type: ignore[import]


def connect_ssh(
        host: str,
        port: int = 22,
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        public_key: typing.Optional[str] = None
) -> paramiko.SSHClient:
    client = paramiko.SSHClient()

    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username, password, public_key)

    return client


def run_ssh(
        client: paramiko.SSHClient,
        args: typing.List[str],
        gen_stdin: typing.Generator[bytes, None, None],
        gen_stdout: typing.Generator[None, bytes, None],
        gen_stderr: typing.Generator[None, bytes, None],
        buffer_size: int = 1 << 20,
        join_interval: float = 0.1
) -> typing.Callable[[], int]:
    error = None

    # connect

    channel = client.get_transport().open_session()
    channel.exec_command(' '.join(
        shlex.quote(arg)
        for arg in args
    ))

    # create threads

    def send_stdin() -> None:
        nonlocal error

        try:
            for data in gen_stdin:
                channel.sendall(data)

            channel.shutdown_write()
        except BaseException as raw_error:  # pylint: disable=broad-except
            error = raw_error

    def receive_stdout() -> None:
        nonlocal error

        try:
            next(gen_stdout)
            data = channel.recv(buffer_size)

            while data:
                gen_stdout.send(data)
                data = channel.recv(buffer_size)

            gen_stdout.send(b'')
        except BaseException as raw_error:  # pylint: disable=broad-except
            error = raw_error

    def receive_stderr() -> None:
        nonlocal error

        try:
            next(gen_stderr)
            data = channel.recv_stderr(buffer_size)

            while data:
                gen_stderr.send(data)
                data = channel.recv_stderr(buffer_size)

            gen_stderr.send(b'')
        except BaseException as raw_error:  # pylint: disable=broad-except
            error = raw_error

    stdin_thread = threading.Thread(target=send_stdin)
    stdout_thread = threading.Thread(target=receive_stdout)
    stderr_thread = threading.Thread(target=receive_stderr)

    stdin_thread.start()
    stdout_thread.start()
    stderr_thread.start()

    # join threads

    def join() -> int:
        while error is None and (
                stdin_thread.is_alive()
                or stdout_thread.is_alive()
                or stderr_thread.is_alive()
        ):
            stdin_thread.join(join_interval)
            stdout_thread.join(join_interval)
            stderr_thread.join(join_interval)

        if error is not None:
            channel.close()

            raise error  # pylint: disable=raising-bad-type

        return channel.recv_exit_status()  # type: ignore[no-any-return]

    return join
