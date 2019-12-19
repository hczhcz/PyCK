import io
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


def file_out(
        path: str
) -> typing.Generator[None, bytes, None]:
    yield from stream_out(open(path, 'wb'))


def echo_io() -> typing.Tuple[typing.BinaryIO, typing.BinaryIO]:
    read_semaphore = threading.Semaphore(0)
    write_semaphore = threading.Semaphore(1)
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

            read_semaphore.acquire()

            assert buffered_data is not None

            size = min(len(data), len(buffered_data))

            if size < len(buffered_data):
                # pylint: disable=unsubscriptable-object
                data[:size] = buffered_data[:size]
                buffered_data = buffered_data[size:]

                read_semaphore.release()
            else:
                data[:size] = buffered_data
                buffered_data = None

                write_semaphore.release()

            return size

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

            write_semaphore.acquire()

            size = len(data)
            buffered_data = data

            read_semaphore.release()

            return size

    # TODO: better solution?
    read_stream = typing.cast(typing.BinaryIO, ReadIO())
    write_stream = typing.cast(typing.BinaryIO, WriteIO())

    return read_stream, write_stream
