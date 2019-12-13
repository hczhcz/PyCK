import subprocess
import threading
import typing


def run_process(
        args: typing.List[str],
        gen_stdin: typing.Generator[bytes, None, None],
        gen_stdout: typing.Generator[None, bytes, None],
        gen_stderr: typing.Generator[None, bytes, None],
        buffer_size: int = 1 << 20,
        join_interval: float = 0.1
) -> typing.Callable[[], int]:
    error = None

    # connect

    process = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # create threads

    def send_stdin() -> None:
        nonlocal error

        try:
            for data in gen_stdin:
                process.stdin.write(data)

            process.stdin.close()
        except Exception as raw_error:
            error = raw_error

    def receive_stdout() -> None:
        nonlocal error

        try:
            next(gen_stdout)
            data = process.stdout.read(buffer_size)

            while data:
                gen_stdout.send(data)
                data = process.stdout.read(buffer_size)
        except Exception as raw_error:
            error = raw_error

    def receive_stderr() -> None:
        nonlocal error

        try:
            next(gen_stderr)
            data = process.stderr.read(buffer_size)

            while data:
                gen_stderr.send(data)
                data = process.stderr.read(buffer_size)
        except Exception as raw_error:
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
            process.kill()

            raise error

        return process.wait()

    return join
