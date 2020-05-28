import dis
import functools
import inspect
import typing

from ck import exception
from ck.query import ast


def sql_template(
        function: typing.Callable[..., typing.Any]
) -> typing.Callable[..., str]:
    closure = inspect.getclosurevars(function)
    signature = inspect.signature(function)

    bytecode = dis.Bytecode(function)
    instructions = list(bytecode)

    def run(
            stack: typing.List[ast.BaseAST],
            context: typing.Dict[str, ast.BaseAST],
            opname: str,
            argval: typing.Any
    ) -> bool:
        # TODO
        # pylint: disable=trailing-comma-tuple

        if opname == 'NOP':
            pass
        elif opname == 'POP_TOP':
            stack.pop()
        elif opname == 'ROT_TWO':
            stack[-2:] = stack[-1], stack[-2]
        elif opname == 'ROT_THREE':
            stack[-3:] = stack[-1], *stack[-3:-1]
        elif opname == 'ROT_FOUR':
            stack[-4:] = stack[-1], *stack[-4:-1]
        elif opname == 'DUP_TOP':
            stack.append(stack[-1])
        elif opname == 'DUP_TOP_TWO':
            stack.extend(stack[-2:])
        elif opname == 'UNARY_POSITIVE':
            stack[-1] = ast.Call('negate', [
                ast.Call('negate', stack[-1:]),
            ])
        elif opname == 'UNARY_NEGATIVE':
            stack[-1] = ast.Call('negate', stack[-1:])
        elif opname == 'UNARY_NOT':
            stack[-1] = ast.Call('not', stack[-1:])
        elif opname == 'UNARY_INVERT':
            stack[-1] = ast.Call('bitNot', stack[-1:])
        elif opname == 'GET_ITER':
            stack[-1] = ast.Value(iter(ast.unbox(stack[-1])))
        elif opname == 'GET_YIELD_FROM_ITER':
            # TODO: more accurate semantic
            stack[-1] = ast.Value(iter(ast.unbox(stack[-1])))
        elif opname == 'BINARY_POWER':
            stack[-2:] = ast.Call('pow', stack[-2:]),
        elif opname == 'BINARY_MULTIPLY':
            stack[-2:] = ast.Call('multiply', stack[-2:]),
        elif opname == 'BINARY_MATRIX_MULTIPLY':
            stack[-2:] = ast.Call('cast', stack[-2:]),
        elif opname == 'BINARY_FLOOR_DIVIDE':
            stack[-2:] = ast.Call('intDiv', stack[-2:]),
        elif opname == 'BINARY_TRUE_DIVIDE':
            stack[-2:] = ast.Call('divide', stack[-2:]),
        elif opname == 'BINARY_MODULO':
            stack[-2:] = ast.Call('modulo', stack[-2:]),
        elif opname == 'BINARY_ADD':
            stack[-2:] = ast.Call('plus', stack[-2:]),
        elif opname == 'BINARY_SUBTRACT':
            stack[-2:] = ast.Call('minus', stack[-2:]),
        elif opname == 'BINARY_SUBSCR':
            stack[-2:] = ast.Call('arrayElement', stack[-2:]),
        elif opname == 'BINARY_LSHIFT':
            stack[-2:] = ast.Call('bitShiftLeft', stack[-2:]),
        elif opname == 'BINARY_RSHIFT':
            stack[-2:] = ast.Call('bitShiftRight', stack[-2:]),
        elif opname == 'BINARY_AND':
            stack[-2:] = ast.Call('bitAnd', stack[-2:]),
        elif opname == 'BINARY_XOR':
            stack[-2:] = ast.Call('bitXor', stack[-2:]),
        elif opname == 'BINARY_OR':
            stack[-2:] = ast.Call('bitOr', stack[-2:]),
        elif opname == 'INPLACE_POWER':
            stack[-2:] = ast.Call('pow', stack[-2:]),
        elif opname == 'INPLACE_MULTIPLY':
            stack[-2:] = ast.Call('multiply', stack[-2:]),
        elif opname == 'INPLACE_MATRIX_MULTIPLY':
            stack[-2:] = ast.Call('cast', stack[-2:]),
        elif opname == 'INPLACE_FLOOR_DIVIDE':
            stack[-2:] = ast.Call('intDiv', stack[-2:]),
        elif opname == 'INPLACE_TRUE_DIVIDE':
            stack[-2:] = ast.Call('divide', stack[-2:]),
        elif opname == 'INPLACE_MODULO':
            stack[-2:] = ast.Call('modulo', stack[-2:]),
        elif opname == 'INPLACE_ADD':
            stack[-2:] = ast.Call('plus', stack[-2:]),
        elif opname == 'INPLACE_SUBTRACT':
            stack[-2:] = ast.Call('minus', stack[-2:]),
        elif opname == 'INPLACE_LSHIFT':
            stack[-2:] = ast.Call('bitShiftLeft', stack[-2:]),
        elif opname == 'INPLACE_RSHIFT':
            stack[-2:] = ast.Call('bitShiftRight', stack[-2:]),
        elif opname == 'INPLACE_AND':
            stack[-2:] = ast.Call('bitAnd', stack[-2:]),
        elif opname == 'INPLACE_XOR':
            stack[-2:] = ast.Call('bitXor', stack[-2:]),
        elif opname == 'INPLACE_OR':
            stack[-2:] = ast.Call('bitOr', stack[-2:]),
        elif opname == 'STORE_SUBSCR':
            stack[-3:] = ast.Call('arrayConcat', [
                ast.Call('arraySlice', [
                    stack[-2],
                    ast.Value(1),
                    ast.Call('minus', [
                        stack[-1],
                        ast.Value(1),
                    ]),
                ]),
                ast.Call('array', [stack[-3]]),
                ast.Call('arraySlice', [
                    stack[-2],
                    ast.Call('plus', [
                        stack[-1],
                        ast.Value(1),
                    ]),
                ]),
            ]),
        elif opname == 'DELETE_SUBSCR':
            stack[-2:] = ast.Call('arrayConcat', [
                ast.Call('arraySlice', [
                    stack[-2],
                    ast.Value(1),
                    ast.Call('minus', [
                        stack[-1],
                        ast.Value(1),
                    ]),
                ]),
                ast.Call('arraySlice', [
                    stack[-2],
                    ast.Call('plus', [
                        stack[-1],
                        ast.Value(1),
                    ]),
                ]),
            ]),
        elif opname == 'GET_AWAITABLE':
            raise exception.DisError(opname, argval)
        elif opname == 'GET_AITER':
            raise exception.DisError(opname, argval)
        elif opname == 'GET_ANEXT':
            raise exception.DisError(opname, argval)
        elif opname == 'END_ASYNC_FOR':
            raise exception.DisError(opname, argval)
        elif opname == 'BEFORE_ASYNC_WITH':
            raise exception.DisError(opname, argval)
        elif opname == 'SETUP_ASYNC_WITH':
            raise exception.DisError(opname, argval)
        elif opname == 'PRINT_EXPR':
            raise exception.DisError(opname, argval)
        elif opname == 'SET_ADD':
            value = stack.pop()

            ast.unbox(stack[-argval]).add(value)
        elif opname == 'LIST_APPEND':
            value = stack.pop()

            ast.unbox(stack[-argval]).append(value)
        elif opname == 'MAP_ADD':
            # pylint: disable=unbalanced-tuple-unpacking
            name, value = stack[-2:]
            del stack[-2:]

            ast.unbox(stack[-argval])[name] = value
        elif opname == 'RETURN_VALUE':
            return True
        elif opname == 'YIELD_VALUE':
            raise exception.DisError(opname, argval)
        elif opname == 'YIELD_FROM':
            raise exception.DisError(opname, argval)
        elif opname == 'SETUP_ANNOTATIONS':
            raise exception.DisError(opname, argval)
        elif opname == 'IMPORT_STAR':
            context.update({
                name: ast.box(value)
                for name, value in __import__(argval).__dict__.items()
                if not name.startswith('_')
            })
        elif opname == 'POP_BLOCK':
            raise exception.DisError(opname, argval)
        elif opname == 'POP_EXCEPT':
            raise exception.DisError(opname, argval)
        elif opname == 'POP_FINALLY':
            raise exception.DisError(opname, argval)
        elif opname == 'BEGIN_FINALLY':
            raise exception.DisError(opname, argval)
        elif opname == 'END_FINALLY':
            raise exception.DisError(opname, argval)
        elif opname == 'LOAD_BUILD_CLASS':
            raise exception.DisError(opname, argval)
        elif opname == 'SETUP_WITH':
            raise exception.DisError(opname, argval)
        elif opname == 'WITH_CLEANUP_START':
            raise exception.DisError(opname, argval)
        elif opname == 'WITH_CLEANUP_FINISH':
            raise exception.DisError(opname, argval)
        elif opname == 'STORE_NAME':
            context[argval] = stack.pop()
        elif opname == 'DELETE_NAME':
            del context[argval]
        elif opname == 'UNPACK_SEQUENCE':
            stack[-1:] = ast.unbox(stack[-1])[:argval]
        elif opname == 'UNPACK_EX':
            if argval // 256:
                stack[-1:] = (
                    *ast.unbox(stack[-1])[:argval % 256],
                    ast.unbox(stack[-1])[argval % 256:-argval // 256],
                    *ast.unbox(stack[-1])[-argval // 256:],
                )
            else:
                stack[-1:] = (
                    *ast.unbox(stack[-1])[:argval % 256],
                    ast.unbox(stack[-1])[argval % 256:],
                )
        elif opname == 'STORE_ATTR':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'DELETE_ATTR':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'STORE_GLOBAL':
            context[argval] = stack.pop()
        elif opname == 'DELETE_GLOBAL':
            del context[argval]
        elif opname == 'LOAD_CONST':
            stack.append(ast.Value(argval))
        elif opname == 'LOAD_NAME':
            if argval in context:
                stack.append(context[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'BUILD_TUPLE':
            stack[len(stack) - argval:] = ast.Value(
                tuple(
                    ast.unbox(value)
                    for value in stack[len(stack) - argval:]
                )
            ),
        elif opname == 'BUILD_LIST':
            stack[len(stack) - argval:] = ast.Value(
                [
                    ast.unbox(value)
                    for value in stack[len(stack) - argval:]
                ]
            ),
        elif opname == 'BUILD_SET':
            stack[len(stack) - argval:] = ast.Value(
                {
                    ast.unbox(value)
                    for value in stack[len(stack) - argval:]
                }
            ),
        elif opname == 'BUILD_MAP':
            stack[len(stack) - 2 * argval:] = ast.Value(
                dict(
                    zip(
                        stack[len(stack) - 2 * argval::2],
                        (
                            ast.unbox(value)
                            for value in stack[len(stack) - 2 * argval + 1::2]
                        )
                    )
                )
            ),
        elif opname == 'BUILD_CONST_KEY_MAP':
            stack[-argval - 1:] = ast.Value(
                dict(
                    zip(
                        ast.unbox(stack[-1]),
                        (
                            ast.unbox(value)
                            for value in stack[-argval - 1:-1]
                        )
                    )
                )
            ),
        elif opname == 'BUILD_STRING':
            stack[len(stack) - argval:] = ast.Value(
                ''.join(
                    ast.unbox(value)
                    for value in stack[len(stack) - argval:]
                )
            ),
        elif opname == 'BUILD_TUPLE_UNPACK':
            stack[len(stack) - argval:] = ast.Value(
                tuple(
                    member
                    for value in stack[len(stack) - argval:]
                    for member in ast.unbox(value)
                )
            ),
        elif opname == 'BUILD_TUPLE_UNPACK_WITH_CALL':
            stack[len(stack) - argval:] = ast.Value(
                tuple(
                    member
                    for value in stack[len(stack) - argval:]
                    for member in ast.unbox(value)
                )
            ),
        elif opname == 'BUILD_LIST_UNPACK':
            stack[len(stack) - argval:] = ast.Value(
                [
                    member
                    for value in stack[len(stack) - argval:]
                    for member in ast.unbox(value)
                ]
            ),
        elif opname == 'BUILD_SET_UNPACK':
            stack[len(stack) - argval:] = ast.Value(
                {
                    member
                    for value in stack[len(stack) - argval:]
                    for member in ast.unbox(value)
                }
            ),
        elif opname == 'BUILD_MAP_UNPACK':
            stack[len(stack) - argval:] = ast.Value(
                dict(
                    member
                    for value in stack[len(stack) - argval:]
                    for member in ast.unbox(value).items()
                )
            ),
        elif opname == 'BUILD_MAP_UNPACK_WITH_CALL':
            stack[len(stack) - argval:] = ast.Value(
                dict(
                    member
                    for value in stack[len(stack) - argval:]
                    for member in ast.unbox(value).items()
                )
            ),
        elif opname == 'LOAD_ATTR':
            if isinstance(stack[-1], ast.BaseStatement):
                stack[-1] = ast.SimpleClause(stack[-1], argval)
            else:
                stack[-1] = ast.Call('tupleElement', [
                    stack[-1],
                    ast.Value(argval),
                ])
        elif opname == 'COMPARE_OP':
            # notice: see dis.cmp_op
            if argval == '<':
                stack[-2:] = ast.Call('less', stack[-2:]),
            elif argval == '<=':
                stack[-2:] = ast.Call('lessOrEquals', stack[-2:]),
            elif argval == '==':
                stack[-2:] = ast.Call('equals', stack[-2:]),
            elif argval == '!=':
                stack[-2:] = ast.Call('notEquals', stack[-2:]),
            elif argval == '>':
                stack[-2:] = ast.Call('greater', stack[-2:]),
            elif argval == '>=':
                stack[-2:] = ast.Call('greaterOrEquals', stack[-2:]),
            elif argval == 'in':
                stack[-2:] = ast.Call('in', stack[-2:]),
            elif argval == 'not in':
                stack[-2:] = ast.Call('notIn', stack[-2:]),
            elif argval == 'is':
                stack[-2:] = ast.Call('and', [
                    ast.Call('equals', [
                        ast.Call('toTypeName', stack[-2:-1]),
                        ast.Call('toTypeName', stack[-1:]),
                    ]),
                    ast.Call('equals', stack[-2:]),
                ]),
            elif argval == 'is not':
                stack[-2:] = ast.Call('or', [
                    ast.Call('notEquals', [
                        ast.Call('toTypeName', stack[-2:-1]),
                        ast.Call('toTypeName', stack[-1:]),
                    ]),
                    ast.Call('notEquals', stack[-2:]),
                ]),
            else:
                raise exception.DisError(opname, argval)
        elif opname == 'IMPORT_NAME':
            stack[-2:] = ast.Value(
                __import__(
                    argval,
                    fromlist=ast.unbox(stack[-2]),
                    level=ast.unbox(stack[-1])
                ).__dict__
            ),
        elif opname == 'IMPORT_FROM':
            context[argval] = ast.box(ast.unbox(stack[-1])[argval])
        elif opname == 'JUMP_FORWARD':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'POP_JUMP_IF_TRUE':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'POP_JUMP_IF_FALSE':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'JUMP_IF_TRUE_OR_POP':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'JUMP_IF_FALSE_OR_POP':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'JUMP_ABSOLUTE':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'FOR_ITER':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'LOAD_GLOBAL':
            if argval in context:
                stack.append(context[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'SETUP_FINALLY':
            raise exception.DisError(opname, argval)
        elif opname == 'CALL_FINALLY':
            raise exception.DisError(opname, argval)
        elif opname == 'LOAD_FAST':
            if argval in context:
                stack.append(context[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'STORE_FAST':
            context[argval] = stack.pop()
        elif opname == 'DELETE_FAST':
            del context[argval]
        elif opname == 'LOAD_CLOSURE':
            # TODO: reference is not yet supported
            if argval in context:
                stack.append(context[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'LOAD_DEREF':
            if argval in context:
                stack.append(context[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'LOAD_CLASSDEREF':
            if argval in context:
                stack.append(context[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'STORE_DEREF':
            context[argval] = stack.pop()
        elif opname == 'DELETE_DEREF':
            del context[argval]
        elif opname == 'RAISE_VARARGS':
            raise exception.DisError(opname, argval)
        elif opname == 'CALL_FUNCTION':
            if isinstance(stack[-argval - 1], ast.Identifier):
                stack[-argval - 1:] = ast.Call(
                    stack[-argval - 1],
                    stack[len(stack) - argval:]
                ),
            elif isinstance(stack[-argval - 1], ast.Call):
                stack[-argval - 1:] = ast.Call(
                    stack[-argval - 1],
                    stack[len(stack) - argval:]
                ),
            elif isinstance(stack[-argval - 1], ast.BaseStatement):
                stack[-argval - 1:] = ast.ListClause(
                    stack[-argval - 1],
                    stack[len(stack) - argval:]
                ),
            else:
                stack[-argval - 1] = ast.box(
                    ast.unbox(stack[-argval - 1])(
                        *(
                            ast.unbox(value)
                            for value in stack[len(stack) - argval:]
                        )
                    )
                )
        elif opname == 'CALL_FUNCTION_KW':
            names = ast.unbox(stack[-1])

            if isinstance(stack[-argval - 2], ast.Identifier):
                if names:
                    raise exception.DisError(opname, argval)

                stack[-argval - 2:] = ast.Call(
                    stack[-argval - 2],
                    stack[-argval - 1:-1]
                ),
            elif isinstance(stack[-argval - 2], ast.Call):
                if names:
                    raise exception.DisError(opname, argval)

                stack[-argval - 2:] = ast.Call(
                    stack[-argval - 2],
                    stack[-argval - 1:-1]
                ),
            elif isinstance(stack[-argval - 2], ast.BaseStatement):
                if names:
                    raise exception.DisError(opname, argval)

                stack[-argval - 2:] = ast.ListClause(
                    stack[-argval - 2],
                    stack[-argval - 1:-1]
                ),
            else:
                stack[-argval - 2:] = ast.box(
                    ast.unbox(stack[-argval - 2])(
                        *(
                            ast.unbox(value)
                            for value in stack[-argval - 1:-len(names) - 1]
                        ),
                        **dict(
                            zip(
                                names,
                                (
                                    ast.unbox(value)
                                    for value in stack[-len(names) - 1:-1]
                                )
                            )
                        )
                    )
                ),
        elif opname == 'CALL_FUNCTION_EX':
            if argval & 1:
                kw_arguments = ast.unbox(stack[-1])
                stack.pop()
            else:
                kw_arguments = {}

            if isinstance(stack[-2], ast.Identifier):
                if kw_arguments:
                    raise exception.DisError(opname, argval)

                stack[-2:] = ast.Call(stack[-2], ast.unbox(stack[-1])),
            elif isinstance(stack[-2], ast.Call):
                if kw_arguments:
                    raise exception.DisError(opname, argval)

                stack[-2:] = ast.Call(stack[-2], ast.unbox(stack[-1])),
            elif isinstance(stack[-2], ast.BaseStatement):
                if kw_arguments:
                    raise exception.DisError(opname, argval)

                stack[-2:] = ast.ListClause(stack[-2], ast.unbox(stack[-1])),
            else:
                stack[-2:] = ast.box(
                    ast.unbox(stack[-2])(
                        *(
                            ast.unbox(value)
                            for value in ast.unbox(stack[-1])
                        ),
                        **(
                            ast.unbox(value)
                            for value in kw_arguments
                        )
                    )
                ),
        elif opname == 'LOAD_METHOD':
            if isinstance(stack[-1], ast.BaseStatement):
                stack[-1:] = ast.SimpleClause(stack[-1], argval), stack[-1]
            else:
                stack[-1:] = ast.Value(
                    ast.unbox(stack[-1]).__getattribute__(argval).__func__
                ), stack[-1]
        elif opname == 'CALL_METHOD':
            if isinstance(stack[-argval - 2], ast.BaseStatement):
                stack[-argval - 2:] = ast.ListClause(
                    stack[-argval - 2],
                    stack[len(stack) - argval:]
                ),
            else:
                stack[-argval - 2:] = ast.box(
                    ast.unbox(stack[-argval - 2])(
                        *(
                            ast.unbox(value)
                            for value in stack[-argval - 1:]
                        )
                    )
                ),
        elif opname == 'MAKE_FUNCTION':
            # TODO
            raise exception.DisError(opname, argval)
        elif opname == 'BUILD_SLICE':
            stack[len(stack) - argval:] = ast.Value(
                slice(
                    *(
                        ast.unbox(value)
                        for value in stack[len(stack) - argval:]
                    )
                )
            ),
        elif opname == 'EXTENDED_ARG':
            raise exception.DisError(opname, argval)
        elif opname == 'FORMAT_VALUE':
            if argval & 4:
                spec = ast.unbox(stack[-1])
                stack.pop()
            else:
                spec = ''

            if argval & 3 == 0:
                stack[-1] = ast.Value(format(ast.unbox(stack[-1]), spec))
            elif argval & 3 == 1:
                stack[-1] = ast.Value(format(str(ast.unbox(stack[-1])), spec))
            elif argval & 3 == 2:
                stack[-1] = ast.Value(format(repr(ast.unbox(stack[-1])), spec))
            elif argval & 3 == 3:
                stack[-1] = ast.Value(
                    format(ascii(ast.unbox(stack[-1])), spec)
                )
        elif opname == 'HAVE_ARGUMENT':
            raise exception.DisError(opname, argval)
        else:
            raise exception.DisError(opname, argval)

        return False

    @functools.wraps(function)
    def build(
            *args: typing.Any,
            **kwargs: typing.Any
    ) -> typing.Any:
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()

        stack: typing.List[ast.BaseAST] = []
        context: typing.Dict[str, ast.BaseAST] = {
            'with_': ast.Initial('with'),
            'select': ast.Initial('select'),
            'select_distinct': ast.Initial('select_distinct'),
            'insert_into': ast.Initial('insert_into'),
            **{
                name: ast.box(value)
                for name, value in closure.builtins.items()
            },
            **{
                name: ast.box(value)
                for name, value in closure.globals.items()
            },
            **{
                name: ast.box(value)
                for name, value in closure.nonlocals.items()
            },
            **{
                name: ast.box(value)
                for name, value in bound_arguments.arguments.items()
            },
        }

        # notice: see dis.opmap
        for instruction in instructions:
            done = run(stack, context, instruction.opname, instruction.argval)

            if done:
                return stack.pop()

        raise exception.DisError(None, None)

    return build
