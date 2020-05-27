import abc
import typing


def sql_escape(
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


class IdentifierExpression(BaseExpression):
    def __init__(
            self,
            name: str
    ) -> None:
        self._name = name

    def render_expression(self) -> str:
        return sql_escape(self._name, '`')


class ValueExpression(BaseExpression):
    def __init__(
            self,
            value: typing.Any
    ) -> None:
        self._value = value

    def get(self) -> typing.Any:
        return self._value

    def render_expression(self) -> str:
        if isinstance(self._value, str):
            return sql_escape(self._value, '\'')

        # TODO: other types?
        return str(self._value)


class CallExpression(BaseExpression):
    def __init__(
            self,
            function: typing.Union[
                IdentifierExpression,
                str
            ],
            arguments: typing.List[BaseAST]
    ) -> None:
        self._function = function
        self._arguments = arguments

    def render_expression(self) -> str:
        if isinstance(self._function, IdentifierExpression):
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


class InitialStatement(BaseStatement):
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


class SimpleClauseStatement(BaseStatement):
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


class ListClauseStatement(BaseStatement):
    def __init__(
            self,
            previous: BaseStatement,
            arguments: typing.List[BaseAST]
    ) -> None:
        if isinstance(previous, ListClauseStatement):
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
