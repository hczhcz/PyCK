from ck.iteration import adhoc
from ck.iteration import io


collect_out = adhoc.collect_out  # pylint: disable=invalid-name
concat_in = adhoc.concat_in  # pylint: disable=invalid-name
empty_in = adhoc.empty_in  # pylint: disable=invalid-name
empty_out = adhoc.empty_out  # pylint: disable=invalid-name
given_in = adhoc.given_in  # pylint: disable=invalid-name
ignore_out = adhoc.ignore_out  # pylint: disable=invalid-name
echo_io = io.echo_io  # pylint: disable=invalid-name
file_in = io.file_in  # pylint: disable=invalid-name
file_out = io.file_out  # pylint: disable=invalid-name
stream_in = io.stream_in  # pylint: disable=invalid-name
stream_out = io.stream_out  # pylint: disable=invalid-name
