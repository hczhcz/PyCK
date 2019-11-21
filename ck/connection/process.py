import subprocess
import threading
import types


def run(
    args,
    gen_stdin,
    gen_stdout,
    gen_stderr,
    buffer_size=1 << 20,
    join_interval=0.1
):
    assert type(args) is list
    for arg in args:
        assert type(arg) is str
    assert type(gen_stdin) is types.GeneratorType
    assert type(gen_stdout) is types.GeneratorType
    assert type(gen_stderr) is types.GeneratorType
    assert type(buffer_size) is int
    assert type(join_interval) is int or type(join_interval) is float

    error = None

    # connect

    process = subprocess.Popen(
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # create threads

    def send_stdin():
        nonlocal error

        try:
            for data in gen_stdin:
                assert type(data) is bytes

                process.stdin.write(data)

            process.stdin.close()
        except Exception as e:
            error = e

    def receive_stdout():
        nonlocal error

        try:
            next(gen_stdout)
            data = process.stdout.read(buffer_size)

            while data:
                gen_stdout.send(data)
                data = process.stdout.read(buffer_size)
        except Exception as e:
            error = e

    def receive_stderr():
        nonlocal error

        try:
            next(gen_stderr)
            data = process.stderr.read(buffer_size)

            while data:
                gen_stderr.send(data)
                data = process.stderr.read(buffer_size)
        except Exception as e:
            error = e

    stdin_thread = threading.Thread(target=send_stdin)
    stdout_thread = threading.Thread(target=receive_stdout)
    stderr_thread = threading.Thread(target=receive_stderr)

    stdin_thread.start()
    stdout_thread.start()
    stderr_thread.start()

    # join threads

    def join():
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
