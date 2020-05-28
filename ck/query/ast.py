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


def escape_value(
        value: typing.Any
) -> str:
    if value is None:
        return 'null'

    if isinstance(value, str):
        return escape_text(value, '\'')

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

    if isinstance(value, dict):
        members_text = ', '.join(
            escape_value(member)
            for member in value.items()
        )

        return f'array({members_text})'

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

    if isinstance(value, bool):
        return 'true' if value else 'false'

    if isinstance(value, bytes):
        return escape_buffer(value, '\'')

    if isinstance(value, bytearray):
        return escape_buffer(value, '\'')

    if isinstance(value, memoryview):
        return escape_buffer(value, '\'')

    raise TypeError()


def box(
        value: typing.Any
) -> BaseAST:
    if isinstance(value, BaseAST):
        return value

    return Value(value)


def unbox(
        value: BaseAST
) -> typing.Any:
    if isinstance(value, Value):
        return value._value

    return value


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


class Value(BaseExpression):
    def __init__(
            self,
            value: typing.Any
    ) -> None:
        self._value = value

    def render_expression(self) -> str:
        return escape_value(self._value)


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
            function: typing.Union[
                BaseAST,
                str
            ],
            arguments: typing.List[BaseAST]
    ) -> None:
        self._function = function
        self._arguments = arguments

    def render_expression(self) -> str:
        if isinstance(self._function, BaseAST):
            function_text = self._function.render_expression()
        else:
            function_text = self._function

        arguments_text = ', '.join(
            argument.render_expression()
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
            arguments: typing.List[BaseAST]
    ) -> None:
        if isinstance(previous, ListClause):
            raise TypeError()

        self._previous = previous
        self._arguments = arguments

    def render_statement(self) -> str:
        previous_text = self._previous.render_statement()
        arguments_text = ', '.join(
            argument.render_expression()
            for argument in self._arguments
        )

        return f'{previous_text} {arguments_text}'
