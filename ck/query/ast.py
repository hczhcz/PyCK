import abc
import typing


def escape_text(
        text: str,
        quote: str
) -> str:
    result = ''

    for char in text:
        if char == '\x00':
            result += '\\0'
        elif char == '\\':
            result += '\\\\'
        elif char == '\a':
            result += '\\a'
        elif char == '\b':
            result += '\\b'
        elif char == '\f':
            result += '\\f'
        elif char == '\n':
            result += '\\n'
        elif char == '\r':
            result += '\\r'
        elif char == '\t':
            result += '\\t'
        elif char == '\v':
            result += '\\v'
        elif char == quote:
            result += f'\\{quote}'
        else:
            result += char

    return f'{quote}{result}{quote}'


def escape_buffer(
        buffer: typing.Union[
            bytes,
            bytearray,
            memoryview
        ],
        quote: str
) -> str:
    result = ''

    for char in buffer:
        if char == 0:
            result += '\\0'
        elif char == ord('\\'):
            result += '\\\\'
        elif char == ord('\a'):
            result += '\\a'
        elif char == ord('\b'):
            result += '\\b'
        elif char == ord('\f'):
            result += '\\f'
        elif char == ord('\n'):
            result += '\\n'
        elif char == ord('\r'):
            result += '\\r'
        elif char == ord('\t'):
            result += '\\t'
        elif char == ord('\v'):
            result += '\\v'
        elif char == ord(quote):
            result += f'\\{quote}'
        elif char >= 128:
            result += f'\\x{char:02x}'
        else:
            result += chr(char)

    return f'{quote}{result}{quote}'


def escape_value(  # pylint: disable=too-many-return-statements
        value: typing.Any
) -> str:
    if value is None:
        return 'null'

    if value is Ellipsis:
        return '*'

    if isinstance(value, bool):
        return 'true' if value else 'false'

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        return str(value)

    if isinstance(value, complex):
        return f'tuple({value.real}, {value.imag})'

    if isinstance(value, list):
        members_text = ', '.join(
            escape_value(member)
            for member in value
        )

        return f'array({members_text})'

    if isinstance(value, tuple):
        members_text = ', '.join(
            escape_value(member)
            for member in value
        )

        return f'tuple({members_text})'

    if isinstance(value, range):
        return f'range({value.start}, {value.stop}, {value.step})'

    if isinstance(value, str):
        return escape_text(value, '\'')

    if isinstance(value, bytes):
        return escape_buffer(value, '\'')

    if isinstance(value, bytearray):
        return escape_buffer(value, '\'')

    if isinstance(value, memoryview):
        return escape_buffer(value, '\'')

    if isinstance(value, set):
        members_text = ', '.join(
            escape_value(member)
            for member in value
        )

        return f'array({members_text})'

    if isinstance(value, frozenset):
        members_text = ', '.join(
            escape_value(member)
            for member in value
        )

        return f'array({members_text})'

    if isinstance(value, dict):
        members_text = ', '.join(
            escape_value(member)
            for member in value.items()
        )

        return f'array({members_text})'

    if isinstance(value, BaseAST):
        return value.render_expression()

    # TODO: function?
    # if ...:
    #     return 'lambda(tuple(x, y), expr)'

    raise TypeError()


class BaseAST(abc.ABC):
    @abc.abstractmethod
    def render_expression(self) -> str:
        pass

    @abc.abstractmethod
    def render_statement(self) -> str:
        pass


class BaseExpression(BaseAST):
    def render_statement(self) -> str:
        return f'select {self.render_expression()}'


class Identifier(BaseExpression):
    def __init__(
            self,
            name: str
    ) -> None:
        self._name = name

    def render_expression(self) -> str:
        return escape_text(self._name, '`')


class Call(BaseExpression):
    def __init__(
            self,
            function: typing.Any,
            arguments: typing.List[typing.Any]
    ) -> None:
        self._function = function
        self._arguments = arguments

    def render_expression(self) -> str:
        function_text = escape_value(self._function)

        arguments_text = ', '.join(
            escape_value(argument)
            for argument in self._arguments
        )

        return f'{function_text}({arguments_text})'


class BaseStatement(BaseAST):
    def render_expression(self) -> str:
        return f'({self.render_statement()})'


class Initial(BaseStatement):
    def __init__(
            self,
            name: str
    ) -> None:
        self._name = name

    def render_statement(self) -> str:
        name_text = ' '.join(
            part
            for part in self._name.split('_')
            if part
        )

        return name_text


class SimpleClause(BaseStatement):
    def __init__(
            self,
            previous: BaseStatement,
            name: str
    ) -> None:
        self._previous = previous
        self._name = name

    def render_statement(self) -> str:
        previous_text = self._previous.render_statement()
        name_text = ' '.join(
            part
            for part in self._name.split('_')
            if part
        )

        return f'{previous_text} {name_text}'


class ListClause(BaseStatement):
    def __init__(
            self,
            previous: BaseStatement,
            arguments: typing.List[typing.Any],
            kw_arguments: typing.Dict[str, typing.Any]
    ) -> None:
        self._previous = previous
        self._arguments = arguments
        self._kw_arguments = kw_arguments

    def render_statement(self) -> str:
        previous_text = self._previous.render_statement()
        arguments_text = ', '.join(
            (
                *(
                    escape_value(argument)
                    for argument in self._arguments
                ),
                *(
                    f'''{escape_value(value)} as {escape_text(name, '`')}'''
                    for name, value in self._kw_arguments.items()
                ),
            )
        )

        # TODO: handle "create table" separately
        if isinstance(self._previous, ListClause):
            return f'{previous_text} ({arguments_text})'

        if arguments_text:
            return f'{previous_text} {arguments_text}'

        return previous_text
