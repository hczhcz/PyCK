import paramiko
import shlex
import threading
import types


def connect(
    host,
    port=22,
    username=None,
    password=None,
    public_key=None
):
    assert type(host) is str
    assert type(port) is int
    assert username is None or type(username) is str
    assert password is None or type(password) is str
    assert public_key is None or type(public_key) is str

    client = paramiko.SSHClient()

    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username, password, public_key)

    return client


def run(
    client,
    args,
    gen_stdin,
    gen_stdout,
    gen_stderr,
    buffer_size=1 << 20,
    join_interval=0.1
):
    assert type(client) is paramiko.SSHClient
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

    channel = client.get_transport().open_session()
    channel.exec_command(' '.join(
        shlex.quote(arg)
        for arg in args
    ))

    # create threads

    def send_stdin():
        nonlocal error

        try:
            for data in gen_stdin:
                assert type(data) is bytes

                channel.sendall(data)

            channel.shutdown_write()
        except Exception as e:
            error = e

    def receive_stdout():
        nonlocal error

        try:
            next(gen_stdout)
            data = channel.recv(buffer_size)

            while data:
                gen_stdout.send(data)
                data = channel.recv(buffer_size)
        except Exception as e:
            error = e

    def receive_stderr():
        nonlocal error

        try:
            next(gen_stderr)
            data = channel.recv_stderr(buffer_size)

            while data:
                gen_stderr.send(data)
                data = channel.recv_stderr(buffer_size)
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
            channel.close()

            raise error

        return channel.recv_exit_status()

    return join
