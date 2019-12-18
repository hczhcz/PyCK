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


class EchoIO(io.RawIOBase):
    def __init__(self) -> None:
        super().__init__()

        self._read_semaphore = threading.Semaphore(0)
        self._write_semaphore = threading.Semaphore(1)
        self._data: typing.Optional[bytes] = None

    def readable(self) -> bool:
        return True

    def writeable(self) -> bool:
        return True

    def readinto(
            self,
            data: bytearray
    ) -> int:
        if self.closed and self._data is None:
            return 0

        self._read_semaphore.acquire()

        assert self._data is not None

        size = min(len(data), len(self._data))

        if size < len(self._data):
            data[:size] = self._data[:size]
            self._data = self._data[size:]

            self._read_semaphore.release()
        else:
            data[:size] = self._data
            self._data = None

            self._write_semaphore.release()

        return size

    def write(
            self,
            data: bytes
    ) -> int:
        if self.closed:
            raise ValueError()

        self._write_semaphore.acquire()

        size = len(data)
        self._data = data

        self._read_semaphore.release()

        return size
