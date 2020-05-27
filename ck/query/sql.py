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
                assert isinstance(stack[-1], ast.ValueExpression)

                stack[-1] = ast.ValueExpression(iter(stack[-1]))
            elif opname == 'GET_YIELD_FROM_ITER':
                assert isinstance(stack[-1], ast.ValueExpression)

                # notice: not sure
                stack[-1] = ast.ValueExpression(iter(stack[-1]))
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
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'LIST_APPEND':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'MAP_ADD':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'RETURN_VALUE':
                return stack.pop()
            elif opname == 'YIELD_VALUE':
                raise exception.DisError(instruction)
            elif opname == 'YIELD_FROM':
                raise exception.DisError(instruction)
            elif opname == 'SETUP_ANNOTATIONS':
                raise exception.DisError(instruction)
            elif opname == 'IMPORT_STAR':
                raise exception.DisError(instruction)
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
                stack[-1:] = stack[-1][:argval]
            elif opname == 'UNPACK_EX':
                stack[-1:] = (
                    *stack[-1][:argval % 256],
                    stack[-1][argval % 256:-argval // 256],
                    *stack[-1][-argval // 256:],
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
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []

                stack.append(ast.CallExpression('tuple', arguments))
            elif opname == 'BUILD_LIST':
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []

                stack.append(ast.CallExpression('array', arguments))
            elif opname == 'BUILD_SET':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'BUILD_MAP':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'BUILD_CONST_KEY_MAP':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'BUILD_STRING':
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []

                stack.append(ast.CallExpression('concat', arguments))
            elif opname == 'BUILD_TUPLE_UNPACK':
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []

                # TODO: not implemented in ClickHouse
                stack.append(ast.CallExpression('tupleConcat', arguments))
            elif opname == 'BUILD_TUPLE_UNPACK_WITH_CALL':
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []

                # TODO: not implemented in ClickHouse
                stack.append(ast.CallExpression('tupleConcat', arguments))
            elif opname == 'BUILD_LIST_UNPACK':
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []

                stack.append(ast.CallExpression('arrayConcat', arguments))
            elif opname == 'BUILD_SET_UNPACK':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'BUILD_MAP_UNPACK':
                # TODO
                raise exception.DisError(instruction)
            elif opname == 'BUILD_MAP_UNPACK_WITH_CALL':
                # TODO
                raise exception.DisError(instruction)
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
                    stack[-2:] = ast.CallExpression('lessOrEquals', stack[-2:]),
                elif argval == '==':
                    stack[-2:] = ast.CallExpression('equals', stack[-2:]),
                elif argval == '!=':
                    stack[-2:] = ast.CallExpression('notEquals', stack[-2:]),
                elif argval == '>':
                    stack[-2:] = ast.CallExpression('greater', stack[-2:]),
                elif argval == '>=':
                    stack[-2:] = ast.CallExpression('greaterOrEquals', stack[-2:]),
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
                raise exception.DisError(instruction)
            elif opname == 'IMPORT_FROM':
                raise exception.DisError(instruction)
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
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []

                if isinstance(stack[-1], ast.IdentifierExpression):
                    stack[-1] = ast.CallExpression(stack[-1], arguments)
                elif isinstance(stack[-1], ast.BaseStatement):
                    stack[-1] = ast.ListClauseStatement(stack[-1], arguments)
                else:
                    # TODO
                    pass
            elif opname == 'CALL_FUNCTION_KW':
                # TODO
                assert isinstance(stack[-1], ast.ValueExpression)
                assert isinstance(stack[-1].get(), tuple)

                names = stack[-1].get()
                arguments = stack[-argval - 1:-len(names) - 1]
                kw_arguments = dict(zip(names, stack[-len(names) - 1:-1]))
                del stack[-argval - 1:]

                if isinstance(stack[-1], ast.IdentifierExpression):
                    if kw_arguments:
                        raise exception.DisError(instruction)

                    stack[-1] = ast.CallExpression(stack[-1], arguments)
                elif isinstance(stack[-1], ast.BaseStatement):
                    if kw_arguments:
                        raise exception.DisError(instruction)

                    stack[-1] = ast.ListClauseStatement(stack[-1], arguments)
                else:
                    # TODO
                    pass
            elif opname == 'CALL_FUNCTION_EX':
                if argval:
                    assert isinstance(stack[-2], ast.ValueExpression)
                    assert isinstance(stack[-2].get(), tuple)
                    assert isinstance(stack[-1], ast.ValueExpression)
                    assert isinstance(stack[-1].get(), dict)

                    arguments = stack[-2].get()
                    kw_arguments = stack[-1].get()
                    del stack[-2:]
                else:
                    assert isinstance(stack[-1], ast.ValueExpression)
                    assert isinstance(stack[-1].get(), tuple)

                    arguments = stack[-1].get()
                    kw_arguments = {}
                    del stack[-1]

                if isinstance(stack[-1]):
                    if kw_arguments:
                        raise exception.DisError(instruction)

                    stack[-1] = ast.CallExpression(stack[-1], arguments)
                elif isinstance(stack[-1], ast.BaseStatement):
                    if kw_arguments:
                        raise exception.DisError(instruction)

                    stack[-1] = ast.ListClauseStatement(stack[-1], arguments)
                else:
                    # TODO
                    pass
            elif opname == 'LOAD_METHOD':
                if isinstance(stack[-1], ast.BaseStatement):
                    stack[-1] = ast.SimpleClauseStatement(stack[-1], argval)
                    stack.append(stack[-1])
                else:
                    # TODO
                    pass
            elif opname == 'CALL_METHOD':
                if argval:
                    arguments = stack[-argval:]
                    del stack[-argval:]
                else:
                    arguments = []
                if isinstance(stack[-2], ast.BaseStatement):
                    # TODO: ?
                    assert stack[-2] is stack[-1]
                    stack[-2:] = ast.ListClauseStatement(stack[-2], arguments),
                else:
                    # TODO
                    pass
            elif opname == 'MAKE_FUNCTION':
                # TODO
                pass
            elif opname == 'BUILD_SLICE':
                # TODO
                pass
            elif opname == 'EXTENDED_ARG':
                # TODO
                pass
            elif opname == 'FORMAT_VALUE':
                # TODO
                pass
            elif opname == 'HAVE_ARGUMENT':
                raise exception.DisError(instruction)
            else:
                raise exception.DisError(instruction)

        raise exception.DisError(instruction)

    return build
