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

    @functools.wraps(function)
    def build(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()

        stack: typing.List[ast.BaseAST] = []
        context: typing.Dict[str, ast.BaseAST] = {
            'with_': ast.InitialStatement('with'),
            'select': ast.InitialStatement('select'),
            'select_distinct': ast.InitialStatement('select_distinct'),
            'insert_into': ast.InitialStatement('insert_into'),
            **{
                name: ast.ValueExpression(value)
                for name, value in closure.builtins.items()
            },
            **{
                name: ast.ValueExpression(value)
                for name, value in closure.globals.items()
            },
            **{
                name: ast.ValueExpression(value)
                for name, value in closure.nonlocals.items()
            },
            **{
                name: ast.ValueExpression(value)
                for name, value in bound_arguments.arguments.items()
            },
        }

        # notice: see dis.opmap
        for instruction in instructions:
            opname = instruction.opname
            argval = instruction.argval

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
                stack[-1] = ast.CallExpression('negate', [
                    ast.CallExpression('negate', stack[-1:]),
                ])
            elif opname == 'UNARY_NEGATIVE':
                stack[-1] = ast.CallExpression('negate', stack[-1:])
            elif opname == 'UNARY_NOT':
                stack[-1] = ast.CallExpression('not', stack[-1:])
            elif opname == 'UNARY_INVERT':
                stack[-1] = ast.CallExpression('bitNot', stack[-1:])
            elif opname == 'GET_ITER':
                stack[-1] = ast.ValueExpression(iter(stack[-1].unbox()))
            elif opname == 'GET_YIELD_FROM_ITER':
                # TODO: more accurate semantic
                stack[-1] = ast.ValueExpression(iter(stack[-1].unbox()))
            elif opname == 'BINARY_POWER':
                stack[-2:] = ast.CallExpression('pow', stack[-2:]),
            elif opname == 'BINARY_MULTIPLY':
                stack[-2:] = ast.CallExpression('multiply', stack[-2:]),
            elif opname == 'BINARY_MATRIX_MULTIPLY':
                stack[-2:] = ast.CallExpression('cast', stack[-2:]),
            elif opname == 'BINARY_FLOOR_DIVIDE':
                stack[-2:] = ast.CallExpression('intDiv', stack[-2:]),
            elif opname == 'BINARY_TRUE_DIVIDE':
                stack[-2:] = ast.CallExpression('divide', stack[-2:]),
            elif opname == 'BINARY_MODULO':
                stack[-2:] = ast.CallExpression('modulo', stack[-2:]),
            elif opname == 'BINARY_ADD':
                stack[-2:] = ast.CallExpression('plus', stack[-2:]),
            elif opname == 'BINARY_SUBTRACT':
                stack[-2:] = ast.CallExpression('minus', stack[-2:]),
            elif opname == 'BINARY_SUBSCR':
                stack[-2:] = ast.CallExpression('arrayElement', stack[-2:]),
            elif opname == 'BINARY_LSHIFT':
                stack[-2:] = ast.CallExpression('bitShiftLeft', stack[-2:]),
            elif opname == 'BINARY_RSHIFT':
                stack[-2:] = ast.CallExpression('bitShiftRight', stack[-2:]),
            elif opname == 'BINARY_AND':
                stack[-2:] = ast.CallExpression('bitAnd', stack[-2:]),
            elif opname == 'BINARY_XOR':
                stack[-2:] = ast.CallExpression('bitXor', stack[-2:]),
            elif opname == 'BINARY_OR':
                stack[-2:] = ast.CallExpression('bitOr', stack[-2:]),
            elif opname == 'INPLACE_POWER':
                stack[-2:] = ast.CallExpression('pow', stack[-2:]),
            elif opname == 'INPLACE_MULTIPLY':
                stack[-2:] = ast.CallExpression('multiply', stack[-2:]),
            elif opname == 'INPLACE_MATRIX_MULTIPLY':
                stack[-2:] = ast.CallExpression('cast', stack[-2:]),
            elif opname == 'INPLACE_FLOOR_DIVIDE':
                stack[-2:] = ast.CallExpression('intDiv', stack[-2:]),
            elif opname == 'INPLACE_TRUE_DIVIDE':
                stack[-2:] = ast.CallExpression('divide', stack[-2:]),
            elif opname == 'INPLACE_MODULO':
                stack[-2:] = ast.CallExpression('modulo', stack[-2:]),
            elif opname == 'INPLACE_ADD':
                stack[-2:] = ast.CallExpression('plus', stack[-2:]),
            elif opname == 'INPLACE_SUBTRACT':
                stack[-2:] = ast.CallExpression('minus', stack[-2:]),
            elif opname == 'INPLACE_LSHIFT':
                stack[-2:] = ast.CallExpression('bitShiftLeft', stack[-2:]),
            elif opname == 'INPLACE_RSHIFT':
                stack[-2:] = ast.CallExpression('bitShiftRight', stack[-2:]),
            elif opname == 'INPLACE_AND':
                stack[-2:] = ast.CallExpression('bitAnd', stack[-2:]),
            elif opname == 'INPLACE_XOR':
                stack[-2:] = ast.CallExpression('bitXor', stack[-2:]),
            elif opname == 'INPLACE_OR':
                stack[-2:] = ast.CallExpression('bitOr', stack[-2:]),
            elif opname == 'STORE_SUBSCR':
                stack[-3:] = ast.CallExpression('arrayConcat', [
                    ast.CallExpression('arraySlice', [
                        stack[-2],
                        ast.ValueExpression(1),
                        ast.CallExpression('minus', [
                            stack[-1],
                            ast.ValueExpression(1),
                        ]),
                    ]),
                    ast.CallExpression('array', [stack[-3]]),
                    ast.CallExpression('arraySlice', [
                        stack[-2],
                        ast.CallExpression('plus', [
                            stack[-1],
                            ast.ValueExpression(1),
                        ]),
                    ]),
                ]),
            elif opname == 'DELETE_SUBSCR':
                stack[-2:] = ast.CallExpression('arrayConcat', [
                    ast.CallExpression('arraySlice', [
                        stack[-2],
                        ast.ValueExpression(1),
                        ast.CallExpression('minus', [
                            stack[-1],
                            ast.ValueExpression(1),
                        ]),
                    ]),
                    ast.CallExpression('arraySlice', [
                        stack[-2],
                        ast.CallExpression('plus', [
                            stack[-1],
                            ast.ValueExpression(1),
                        ]),
                    ]),
                ]),
            elif opname == 'GET_AWAITABLE':
                raise exception.DisError(instruction)
            elif opname == 'GET_AITER':
                raise exception.DisError(instruction)
            elif opname == 'GET_ANEXT':
                raise exception.DisError(instruction)
            elif opname == 'END_ASYNC_FOR':
                raise exception.DisError(instruction)
            elif opname == 'BEFORE_ASYNC_WITH':
                raise exception.DisError(instruction)
            elif opname == 'SETUP_ASYNC_WITH':
                raise exception.DisError(instruction)
            elif opname == 'PRINT_EXPR':
                raise exception.DisError(instruction)
            elif opname == 'SET_ADD':
                value = stack.pop()

                stack[-argval].unbox().add(value)
            elif opname == 'LIST_APPEND':
                value = stack.pop()

                stack[-argval].unbox().append(value)
            elif opname == 'MAP_ADD':
                name, value = stack[-2:]
                del stack[-2:]

                stack[-argval].unbox()[name] = value
            elif opname == 'RETURN_VALUE':
                return stack.pop()
            elif opname == 'YIELD_VALUE':
                raise exception.DisError(instruction)
            elif opname == 'YIELD_FROM':
                raise exception.DisError(instruction)
            elif opname == 'SETUP_ANNOTATIONS':
                raise exception.DisError(instruction)
            elif opname == 'IMPORT_STAR':
                context.update({
                    name: ast.ValueExpression(value)
                    for name, value in __import__(argval).__dict__.items()
                    if not name.startswith('_')
                })
            elif opname == 'POP_BLOCK':
                raise exception.DisError(instruction)
            elif opname == 'POP_EXCEPT':
                raise exception.DisError(instruction)
            elif opname == 'POP_FINALLY':
                raise exception.DisError(instruction)
            elif opname == 'BEGIN_FINALLY':
                raise exception.DisError(instruction)
            elif opname == 'END_FINALLY':
                raise exception.DisError(instruction)
            elif opname == 'LOAD_BUILD_CLASS':
                raise exception.DisError(instruction)
            elif opname == 'SETUP_WITH':
                raise exception.DisError(instruction)
            elif opname == 'WITH_CLEANUP_START':
                raise exception.DisError(instruction)
            elif opname == 'WITH_CLEANUP_FINISH':
                raise exception.DisError(instruction)
            elif opname == 'STORE_NAME':
                context[argval] = stack.pop()
            elif opname == 'DELETE_NAME':
                del context[argval]
            elif opname == 'UNPACK_SEQUENCE':
                stack[-1:] = stack[-1].unbox()[:argval]
            elif opname == 'UNPACK_EX':
                if argval // 256:
                    stack[-1:] = (
                        *stack[-1].unbox()[:argval % 256],
                        stack[-1].unbox()[argval % 256:-argval // 256],
                        *stack[-1].unbox()[-argval // 256:],
                    )
                else:
                    stack[-1:] = (
                        *stack[-1].unbox()[:argval % 256],
                        stack[-1].unbox()[argval % 256:],
                    )
            elif opname == 'STORE_ATTR':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'DELETE_ATTR':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'STORE_GLOBAL':
                context[argval] = stack.pop()
            elif opname == 'DELETE_GLOBAL':
                del context[argval]
            elif opname == 'LOAD_CONST':
                stack.append(ast.ValueExpression(argval))
            elif opname == 'LOAD_NAME':
                if argval in context:
                    stack.append(context[argval])
                else:
                    stack.append(ast.IdentifierExpression(argval))
            elif opname == 'BUILD_TUPLE':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    tuple(
                        value.unbox()
                        for value in stack[len(stack) - argval:]
                    )
                ),
            elif opname == 'BUILD_LIST':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    [
                        value.unbox()
                        for value in stack[len(stack) - argval:]
                    ]
                ),
            elif opname == 'BUILD_SET':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    {
                        value.unbox()
                        for value in stack[len(stack) - argval:]
                    }
                ),
            elif opname == 'BUILD_MAP':
                stack[len(stack) - 2 * argval:] = ast.ValueExpression(
                    dict(
                        zip(
                            stack[len(stack) - 2 * argval::2],
                            (
                                value.unbox()
                                for value in stack[len(stack) - 2 * argval + 1::2]
                            )
                        )
                    )
                ),
            elif opname == 'BUILD_CONST_KEY_MAP':
                stack[-argval - 1:] = ast.ValueExpression(
                    dict(
                        zip(
                            stack[-1].unbox(),
                            (
                                value.unbox()
                                for value in stack[-argval - 1:-1]
                            )
                        )
                    )
                ),
            elif opname == 'BUILD_STRING':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    ''.join(
                        value.unbox()
                        for value in stack[len(stack) - argval:]
                    )
                ),
            elif opname == 'BUILD_TUPLE_UNPACK':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    tuple(
                        member
                        for value in stack[len(stack) - argval:]
                        for member in value.unbox()
                    )
                ),
            elif opname == 'BUILD_TUPLE_UNPACK_WITH_CALL':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    tuple(
                        member
                        for value in stack[len(stack) - argval:]
                        for member in value.unbox()
                    )
                ),
            elif opname == 'BUILD_LIST_UNPACK':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    [
                        member
                        for value in stack[len(stack) - argval:]
                        for member in value.unbox()
                    ]
                ),
            elif opname == 'BUILD_SET_UNPACK':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    {
                        member
                        for value in stack[len(stack) - argval:]
                        for member in value.unbox()
                    }
                ),
            elif opname == 'BUILD_MAP_UNPACK':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    dict(
                        member
                        for value in stack[len(stack) - argval:]
                        for member in value.unbox().items()
                    )
                ),
            elif opname == 'BUILD_MAP_UNPACK_WITH_CALL':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    dict(
                        member
                        for value in stack[len(stack) - argval:]
                        for member in value.unbox().items()
                    )
                ),
            elif opname == 'LOAD_ATTR':
                if isinstance(stack[-1], ast.BaseStatement):
                    stack[-1] = ast.SimpleClauseStatement(stack[-1], argval)
                else:
                    stack[-1] = ast.CallExpression('tupleElement', [
                        stack[-1],
                        ast.ValueExpression(argval),
                    ])
            elif opname == 'COMPARE_OP':
                # notice: see dis.cmp_op
                if argval == '<':
                    stack[-2:] = ast.CallExpression('less', stack[-2:]),
                elif argval == '<=':
                    stack[-2:] = ast.CallExpression(
                        'lessOrEquals',
                        stack[-2:]
                    ),
                elif argval == '==':
                    stack[-2:] = ast.CallExpression('equals', stack[-2:]),
                elif argval == '!=':
                    stack[-2:] = ast.CallExpression('notEquals', stack[-2:]),
                elif argval == '>':
                    stack[-2:] = ast.CallExpression('greater', stack[-2:]),
                elif argval == '>=':
                    stack[-2:] = ast.CallExpression(
                        'greaterOrEquals',
                        stack[-2:]
                    ),
                elif argval == 'in':
                    stack[-2:] = ast.CallExpression('in', stack[-2:]),
                elif argval == 'not in':
                    stack[-2:] = ast.CallExpression('notIn', stack[-2:]),
                elif argval == 'is':
                    stack[-2:] = ast.CallExpression('and', [
                        ast.CallExpression('equals', [
                            ast.CallExpression('toTypeName', stack[-2:-1]),
                            ast.CallExpression('toTypeName', stack[-1:]),
                        ]),
                        ast.CallExpression('equals', stack[-2:]),
                    ]),
                elif argval == 'is not':
                    stack[-2:] = ast.CallExpression('or', [
                        ast.CallExpression('notEquals', [
                            ast.CallExpression('toTypeName', stack[-2:-1]),
                            ast.CallExpression('toTypeName', stack[-1:]),
                        ]),
                        ast.CallExpression('notEquals', stack[-2:]),
                    ]),
                else:
                    raise exception.DisError(instruction)
            elif opname == 'IMPORT_NAME':
                stack[-2:] = ast.ValueExpression(
                    __import__(
                        argval,
                        fromlist=stack[-2].unbox(),
                        level=stack[-1].unbox()
                    ).__dict__
                ),
            elif opname == 'IMPORT_FROM':
                context[argval] = ast.ValueExpression(
                    stack[-1].unbox()[argval]
                )
            elif opname == 'JUMP_FORWARD':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'POP_JUMP_IF_TRUE':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'POP_JUMP_IF_FALSE':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'JUMP_IF_TRUE_OR_POP':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'JUMP_IF_FALSE_OR_POP':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'JUMP_ABSOLUTE':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'FOR_ITER':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'LOAD_GLOBAL':
                if argval in context:
                    stack.append(context[argval])
                else:
                    stack.append(ast.IdentifierExpression(argval))
            elif opname == 'SETUP_FINALLY':
                raise exception.DisError(instruction)
            elif opname == 'CALL_FINALLY':
                raise exception.DisError(instruction)
            elif opname == 'LOAD_FAST':
                if argval in context:
                    stack.append(context[argval])
                else:
                    stack.append(ast.IdentifierExpression(argval))
            elif opname == 'STORE_FAST':
                context[argval] = stack.pop()
            elif opname == 'DELETE_FAST':
                del context[argval]
            elif opname == 'LOAD_CLOSURE':
                # TODO: reference is not yet supported
                if argval in context:
                    stack.append(context[argval])
                else:
                    stack.append(ast.IdentifierExpression(argval))
            elif opname == 'LOAD_DEREF':
                if argval in context:
                    stack.append(context[argval])
                else:
                    stack.append(ast.IdentifierExpression(argval))
            elif opname == 'LOAD_CLASSDEREF':
                if argval in context:
                    stack.append(context[argval])
                else:
                    stack.append(ast.IdentifierExpression(argval))
            elif opname == 'STORE_DEREF':
                context[argval] = stack.pop()
            elif opname == 'DELETE_DEREF':
                del context[argval]
            elif opname == 'RAISE_VARARGS':
                raise exception.DisError(instruction)
            elif opname == 'CALL_FUNCTION':
                if isinstance(stack[-argval - 1], ast.IdentifierExpression):
                    stack[-argval - 1:] = ast.CallExpression(
                        stack[-argval - 1],
                        stack[len(stack) - argval:]
                    ),
                elif isinstance(stack[-argval - 1], ast.CallExpression):
                    stack[-argval - 1:] = ast.CallExpression(
                        stack[-argval - 1],
                        stack[len(stack) - argval:]
                    ),
                elif isinstance(stack[-argval - 1], ast.BaseStatement):
                    stack[-argval - 1:] = ast.ListClauseStatement(
                        stack[-argval - 1],
                        stack[len(stack) - argval:]
                    ),
                else:
                    stack[-argval - 1] = ast.ValueExpression(
                        stack[-argval - 1].unbox()(
                            *(
                                value.unbox()
                                for value in stack[len(stack) - argval:]
                            )
                        )
                    )
            elif opname == 'CALL_FUNCTION_KW':
                names = stack[-1].unbox()

                if isinstance(stack[-argval - 2], ast.IdentifierExpression):
                    if names:
                        raise exception.DisError(instruction)

                    stack[-argval - 2:] = ast.CallExpression(
                        stack[-argval - 2],
                        stack[-argval - 1:-1]
                    ),
                elif isinstance(stack[-argval - 2], ast.CallExpression):
                    if names:
                        raise exception.DisError(instruction)

                    stack[-argval - 2:] = ast.CallExpression(
                        stack[-argval - 2],
                        stack[-argval - 1:-1]
                    ),
                elif isinstance(stack[-argval - 2], ast.BaseStatement):
                    if names:
                        raise exception.DisError(instruction)

                    stack[-argval - 2:] = ast.ListClauseStatement(
                        stack[-argval - 2],
                        stack[-argval - 1:-1]
                    ),
                else:
                    stack[-argval - 2:] = ast.ValueExpression(
                        stack[-argval - 2].unbox()(
                            *(
                                value.unbox()
                                for value in stack[-argval - 1:-len(names) - 1]
                            ),
                            **dict(
                                zip(
                                    names,
                                    (
                                        value.unbox()
                                        for value in stack[-len(names) - 1:-1]
                                    )
                                )
                            )
                        )
                    ),
            elif opname == 'CALL_FUNCTION_EX':
                if argval & 1:
                    kw_arguments = stack[-1].unbox()
                    stack.pop()
                else:
                    kw_arguments = {}

                if isinstance(stack[-2], ast.IdentifierExpression):
                    if kw_arguments:
                        raise exception.DisError(instruction)

                    stack[-2:] = ast.CallExpression(stack[-2], stack[-1].unbox()),
                elif isinstance(stack[-2], ast.CallExpression):
                    if kw_arguments:
                        raise exception.DisError(instruction)

                    stack[-2:] = ast.CallExpression(stack[-2], stack[-1].unbox()),
                elif isinstance(stack[-2], ast.BaseStatement):
                    if kw_arguments:
                        raise exception.DisError(instruction)

                    stack[-2:] = ast.ListClauseStatement(stack[-2], stack[-1].unbox()),
                else:
                    stack[-2:] = ast.ValueExpression(
                        stack[-2].unbox()(
                            *(
                                value.unbox()
                                for value in stack[-1].unbox()
                            ),
                            **(
                                value.unbox()
                                for value in kw_arguments
                            )
                        )
                    ),
            elif opname == 'LOAD_METHOD':
                if isinstance(stack[-1], ast.BaseStatement):
                    stack[-1:] = (
                        ast.SimpleClauseStatement(stack[-1], argval),
                        ast.ValueExpression(None),
                    )
                else:
                    stack[-1:] = ast.ValueExpression(
                        stack[-1].unbox().__getattribute__(argval).__func__
                    ), stack[-1]
            elif opname == 'CALL_METHOD':
                if isinstance(stack[-argval - 2], ast.BaseStatement):
                    stack[-argval - 2:] = ast.ListClauseStatement(
                        stack[-argval - 2],
                        stack[len(stack) - argval:]
                    ),
                else:
                    stack[-argval - 2:] = ast.ValueExpression(
                        stack[-argval - 2].unbox()(
                            *(
                                value.unbox()
                                for value in stack[-argval - 1:]
                            )
                        )
                    ),
            elif opname == 'MAKE_FUNCTION':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'BUILD_SLICE':
                stack[len(stack) - argval:] = ast.ValueExpression(
                    slice(
                        *(
                            value.unbox()
                            for value in stack[len(stack) - argval:]
                        )
                    )
                ),
            elif opname == 'EXTENDED_ARG':
                raise exception.DisError(instruction)
            elif opname == 'FORMAT_VALUE':
                if argval & 4:
                    spec = stack[-1].unbox()
                    stack.pop()
                else:
                    spec = ''

                if argval & 3 == 0:
                    stack[-1] = ast.ValueExpression(
                        format(stack[-1].unbox, spec)
                    )
                elif argval & 3 == 1:
                    stack[-1] = ast.ValueExpression(
                        format(str(stack[-1].unbox), spec)
                    )
                elif argval & 3 == 2:
                    stack[-1] = ast.ValueExpression(
                        format(repr(stack[-1].unbox), spec)
                    )
                elif argval & 3 == 3:
                    stack[-1] = ast.ValueExpression(
                        format(ascii(stack[-1].unbox), spec)
                    )
            elif opname == 'HAVE_ARGUMENT':
                raise exception.DisError(instruction)
            else:
                raise exception.DisError(instruction)

        raise exception.DisError(instruction)

    return build
