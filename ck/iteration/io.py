import io
import sys
import threading
import typing


def stream_in(
        stream: typing.BinaryIO,
        buffer_size: int = 1 << 20
) -> typing.Generator[bytes, None, None]:
    data = stream.read(buffer_size)

    while data:
        yield data

        data = stream.read(buffer_size)


def pipe_in(
        buffer_size: int = 1 << 20
) -> typing.Generator[bytes, None, None]:
    yield from stream_in(sys.stdin.buffer, buffer_size)


def file_in(
        path: str,
        buffer_size: int = 1 << 20
) -> typing.Generator[bytes, None, None]:
    yield from stream_in(open(path, 'rb'), buffer_size)


def stream_out(
        stream: typing.BinaryIO
) -> typing.Generator[None, bytes, None]:
    data = yield

    while data:
        stream.write(data)

        data = yield

    stream.close()

    yield


def pipe_out() -> typing.Generator[None, bytes, None]:
    yield from stream_out(sys.stdout.buffer)


def file_out(
        path: str
) -> typing.Generator[None, bytes, None]:
    yield from stream_out(open(path, 'wb'))


def echo_io() -> typing.Tuple[typing.BinaryIO, typing.BinaryIO]:
    empty_semaphore = threading.Semaphore(1)
    full_semaphore = threading.Semaphore(0)
    buffered_data: typing.Optional[bytes] = None

    class ReadIO(io.RawIOBase):
        def readable(self) -> bool:
            return True

        def readinto(
                self,
                data: bytearray
        ) -> int:
            nonlocal buffered_data

            if self.closed:  # pylint: disable=using-constant-test
                raise ValueError()

            if write_stream.closed and buffered_data is None:
                return 0

            offset = 0

            full_semaphore.acquire()

            while offset < len(data) and not write_stream.closed:
                size = min(len(data) - offset, len(buffered_data))

                if offset + size < len(data):
                    data[offset:offset + size] = buffered_data
                    buffered_data = None

                    empty_semaphore.release()
                    full_semaphore.acquire()
                elif offset + size == len(data):
                    data[offset:] = buffered_data
                    buffered_data = None

                    empty_semaphore.release()
                else:
                    assert buffered_data is not None

                    data[offset:] = buffered_data[:size]
                    buffered_data = buffered_data[size:]

                    full_semaphore.release()

                offset += size

            return offset

    class WriteIO(io.RawIOBase):
        def writable(self) -> bool:
            return True

        def write(
                self,
                data: bytes
        ) -> int:
            nonlocal buffered_data

            if self.closed or read_stream.closed:
                raise ValueError()

            empty_semaphore.acquire()

            size = len(data)
            buffered_data = data

            full_semaphore.release()

            return size

        def close(self) -> None:
            super().close()

            full_semaphore.release()

    # TODO: better solution?
    read_stream = typing.cast(typing.BinaryIO, ReadIO())
    write_stream = typing.cast(typing.BinaryIO, WriteIO())

    return read_stream, write_stream
