from ck.iteration import adhoc
from ck.iteration import io


collect_out = adhoc.collect_out
concat_in = adhoc.concat_in
empty_in = adhoc.empty_in
empty_out = adhoc.empty_out
given_in = adhoc.given_in
ignore_out = adhoc.ignore_out

echo_io = io.echo_io
file_in = io.file_in
file_out = io.file_out
pipe_in = io.pipe_in
pipe_out = io.pipe_out
stream_in = io.stream_in
stream_out = io.stream_out
