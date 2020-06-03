import dis
import functools
import inspect
import types
import typing

from ck import exception
from ck.query import ast


def sql_template(
        function: typing.Callable[..., typing.Any]
) -> typing.Callable[..., typing.Any]:
    signature = inspect.signature(function)
    instructions = list(dis.get_instructions(function))

    def run(
            global_dict: typing.Dict[str, typing.Any],
            local_dict: typing.Dict[str, typing.Any],
            cells: typing.Tuple[typing.Any, ...],
            stack: typing.List[typing.Any],
            opname: str,
            arg: typing.Any,
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
            stack[-1] = iter(stack[-1])
        elif opname == 'GET_YIELD_FROM_ITER':
            # TODO: more accurate semantic
            stack[-1] = iter(stack[-1])
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
            # TODO: subscr for slices?
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
                    1,
                    ast.Call('minus', [stack[-1], 1]),
                ]),
                ast.Call('array', [stack[-3]]),
                ast.Call('arraySlice', [
                    stack[-2],
                    ast.Call('plus', [stack[-1], 1]),
                ]),
            ]),
        elif opname == 'DELETE_SUBSCR':
            stack[-2:] = ast.Call('arrayConcat', [
                ast.Call('arraySlice', [
                    stack[-2],
                    1,
                    ast.Call('minus', [stack[-1], 1]),
                ]),
                ast.Call('arraySlice', [
                    stack[-2],
                    ast.Call('plus', [stack[-1], 1]),
                ]),
            ]),
        elif opname == 'GET_AWAITABLE':
            raise exception.DisError(opname)
        elif opname == 'GET_AITER':
            raise exception.DisError(opname)
        elif opname == 'GET_ANEXT':
            raise exception.DisError(opname)
        elif opname == 'END_ASYNC_FOR':
            raise exception.DisError(opname)
        elif opname == 'BEFORE_ASYNC_WITH':
            raise exception.DisError(opname)
        elif opname == 'SETUP_ASYNC_WITH':
            raise exception.DisError(opname)
        elif opname == 'PRINT_EXPR':
            print(stack.pop())
        elif opname == 'SET_ADD':
            value = stack.pop()

            stack[-arg].add(value)
        elif opname == 'LIST_APPEND':
            value = stack.pop()

            stack[-arg].append(value)
        elif opname == 'MAP_ADD':
            # pylint: disable=unbalanced-tuple-unpacking
            name, value = stack[-2:]
            del stack[-2:]

            stack[-arg][name] = value
        elif opname == 'RETURN_VALUE':
            return True
        elif opname == 'YIELD_VALUE':
            raise exception.DisError(opname)
        elif opname == 'YIELD_FROM':
            raise exception.DisError(opname)
        elif opname == 'SETUP_ANNOTATIONS':
            if '__annotations__' not in local_dict:
                local_dict['__annotations__'] = {}
        elif opname == 'IMPORT_STAR':
            module = stack.pop()

            local_dict.update({
                name: getattr(module, name)
                for name in dir(module)
                if not name.startswith('_')
            })
        elif opname == 'POP_BLOCK':
            raise exception.DisError(opname)
        elif opname == 'POP_EXCEPT':
            raise exception.DisError(opname)
        elif opname == 'POP_FINALLY':
            raise exception.DisError(opname)
        elif opname == 'BEGIN_FINALLY':
            raise exception.DisError(opname)
        elif opname == 'END_FINALLY':
            raise exception.DisError(opname)
        elif opname == 'LOAD_BUILD_CLASS':
            stack.append(__build_class__)
        elif opname == 'SETUP_WITH':
            raise exception.DisError(opname)
        elif opname == 'WITH_CLEANUP_START':
            raise exception.DisError(opname)
        elif opname == 'WITH_CLEANUP_FINISH':
            raise exception.DisError(opname)
        elif opname == 'STORE_NAME':
            local_dict[argval] = stack.pop()
        elif opname == 'DELETE_NAME':
            del local_dict[argval]
        elif opname == 'UNPACK_SEQUENCE':
            if len(stack[-1]) != arg:
                raise ValueError()
            stack[-1:] = stack[-1][::-1]
        elif opname == 'UNPACK_EX':
            if arg // 256:
                stack[-1:] = (
                    *stack[-1][:arg % 256],
                    stack[-1][arg % 256:-arg // 256],
                    *stack[-1][-arg // 256:],
                )
            else:
                stack[-1:] = (
                    *stack[-1][:arg % 256],
                    stack[-1][arg % 256:],
                )
        elif opname == 'STORE_ATTR':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'DELETE_ATTR':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'STORE_GLOBAL':
            global_dict[argval] = stack.pop()
        elif opname == 'DELETE_GLOBAL':
            del global_dict[argval]
        elif opname == 'LOAD_CONST':
            stack.append(argval)
        elif opname == 'LOAD_NAME':
            if argval in local_dict:
                stack.append(local_dict[argval])
            elif argval in global_dict:
                stack.append(global_dict[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'BUILD_TUPLE':
            stack[len(stack) - arg:] = tuple(stack[len(stack) - arg:]),
        elif opname == 'BUILD_LIST':
            stack[len(stack) - arg:] = stack[len(stack) - arg:],
        elif opname == 'BUILD_SET':
            stack[len(stack) - arg:] = set(stack[len(stack) - arg:]),
        elif opname == 'BUILD_MAP':
            stack[len(stack) - 2 * arg:] = dict(
                zip(
                    stack[len(stack) - 2 * arg::2],
                    stack[len(stack) - 2 * arg + 1::2]
                )
            ),
        elif opname == 'BUILD_CONST_KEY_MAP':
            stack[-arg - 1:] = dict(zip(stack[-1], stack[-arg - 1:-1])),
        elif opname == 'BUILD_STRING':
            stack[len(stack) - arg:] = ''.join(stack[len(stack) - arg:]),
        elif opname == 'BUILD_TUPLE_UNPACK':
            stack[len(stack) - arg:] = tuple(
                member
                for value in stack[len(stack) - arg:]
                for member in value
            ),
        elif opname == 'BUILD_TUPLE_UNPACK_WITH_CALL':
            stack[len(stack) - arg:] = tuple(
                member
                for value in stack[len(stack) - arg:]
                for member in value
            ),
        elif opname == 'BUILD_LIST_UNPACK':
            stack[len(stack) - arg:] = [
                member
                for value in stack[len(stack) - arg:]
                for member in value
            ],
        elif opname == 'BUILD_SET_UNPACK':
            stack[len(stack) - arg:] = {
                member
                for value in stack[len(stack) - arg:]
                for member in value
            },
        elif opname == 'BUILD_MAP_UNPACK':
            stack[len(stack) - arg:] = dict(
                member
                for value in stack[len(stack) - arg:]
                for member in value.items()
            ),
        elif opname == 'BUILD_MAP_UNPACK_WITH_CALL':
            stack[len(stack) - arg:] = dict(
                member
                for value in stack[len(stack) - arg:]
                for member in value.items()
            ),
        elif opname == 'LOAD_ATTR':
            if isinstance(stack[-1], ast.BaseStatement):
                stack[-1] = ast.SimpleClause(stack[-1], argval)
            else:
                stack[-1] = ast.Call('tupleElement', [stack[-1], argval])
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
            elif argval == 'exception match':
                raise exception.DisError(opname)
            elif argval == 'BAD':
                raise exception.DisError(opname)
            else:
                raise exception.DisError(opname)
        elif opname == 'IMPORT_NAME':
            stack[-2:] = __import__(
                argval,
                fromlist=stack[-2],
                level=stack[-1]
            ),
        elif opname == 'IMPORT_FROM':
            local_dict[argval] = getattr(stack[-1], argval)
        elif opname == 'JUMP_FORWARD':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'POP_JUMP_IF_TRUE':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'POP_JUMP_IF_FALSE':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'JUMP_IF_TRUE_OR_POP':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'JUMP_IF_FALSE_OR_POP':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'JUMP_ABSOLUTE':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'FOR_ITER':
            # TODO
            raise exception.DisError(opname)
        elif opname == 'LOAD_GLOBAL':
            if argval in global_dict:
                stack.append(global_dict[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'SETUP_FINALLY':
            raise exception.DisError(opname)
        elif opname == 'CALL_FINALLY':
            raise exception.DisError(opname)
        elif opname == 'LOAD_FAST':
            if argval in local_dict:
                stack.append(local_dict[argval])
            else:
                stack.append(ast.Identifier(argval))
        elif opname == 'STORE_FAST':
            local_dict[argval] = stack.pop()
        elif opname == 'DELETE_FAST':
            del local_dict[argval]
        elif opname == 'LOAD_CLOSURE':
            stack.append(cells[arg])
        elif opname == 'LOAD_DEREF':
            stack.append(cells[arg].cell_contents)
        elif opname == 'LOAD_CLASSDEREF':
            stack.append(cells[arg].cell_contents)
        elif opname == 'STORE_DEREF':
            cells[arg].cell_contents = stack.pop()
        elif opname == 'DELETE_DEREF':
            del cells[arg].cell_contents
        elif opname == 'RAISE_VARARGS':
            raise exception.DisError(opname)
        elif opname == 'CALL_FUNCTION':
            if isinstance(stack[-arg - 1], ast.Identifier):
                stack[-arg - 1:] = ast.Call(
                    stack[-arg - 1],
                    stack[len(stack) - arg:]
                ),
            elif isinstance(stack[-arg - 1], ast.Call):
                stack[-arg - 1:] = ast.Call(
                    stack[-arg - 1],
                    stack[len(stack) - arg:]
                ),
            elif isinstance(stack[-arg - 1], ast.BaseStatement):
                stack[-arg - 1:] = ast.ListClause(
                    stack[-arg - 1],
                    stack[len(stack) - arg:]
                ),
            else:
                stack[-arg - 1:] = stack[-arg - 1](*stack[len(stack) - arg:]),
        elif opname == 'CALL_FUNCTION_KW':
            if isinstance(stack[-arg - 2], ast.Identifier):
                if stack[-1]:
                    raise TypeError()

                stack[-arg - 2:] = ast.Call(
                    stack[-arg - 2],
                    stack[-arg - 1:-1]
                ),
            elif isinstance(stack[-arg - 2], ast.Call):
                if stack[-1]:
                    raise TypeError()

                stack[-arg - 2:] = ast.Call(
                    stack[-arg - 2],
                    stack[-arg - 1:-1]
                ),
            elif isinstance(stack[-arg - 2], ast.BaseStatement):
                if stack[-1]:
                    raise TypeError()

                stack[-arg - 2:] = ast.ListClause(
                    stack[-arg - 2],
                    stack[-arg - 1:-1]
                ),
            else:
                stack[-arg - 2:] = stack[-arg - 2](
                    *stack[-arg - 1:-len(stack[-1]) - 1],
                    **dict(zip(stack[-1], stack[-len(stack[-1]) - 1:-1]))
                ),
        elif opname == 'CALL_FUNCTION_EX':
            if arg & 1:
                kw_arguments = stack[-1]
                stack.pop()
            else:
                kw_arguments = {}

            if isinstance(stack[-2], ast.Identifier):
                if kw_arguments:
                    raise TypeError()

                stack[-2:] = ast.Call(stack[-2], stack[-1]),
            elif isinstance(stack[-2], ast.Call):
                if kw_arguments:
                    raise TypeError()

                stack[-2:] = ast.Call(stack[-2], stack[-1]),
            elif isinstance(stack[-2], ast.BaseStatement):
                if kw_arguments:
                    raise TypeError()

                stack[-2:] = ast.ListClause(stack[-2], stack[-1]),
            else:
                stack[-2:] = stack[-2](*stack[-1], **kw_arguments),
        elif opname == 'LOAD_METHOD':
            if isinstance(stack[-1], ast.BaseStatement):
                stack[-1:] = ast.SimpleClause(stack[-1], argval), stack[-1]
            else:
                stack[-1:] = (
                    getattr(stack[-1], argval).__func__,
                    stack[-1],
                )
        elif opname == 'CALL_METHOD':
            if isinstance(stack[-arg - 2], ast.BaseStatement):
                stack[-arg - 2:] = ast.ListClause(
                    stack[-arg - 2],
                    stack[len(stack) - arg:]
                ),
            else:
                stack[-arg - 2:] = stack[-arg - 2](*stack[-arg - 1:]),
        elif opname == 'MAKE_FUNCTION':
            # TODO
            if arg & 8:
                function = types.FunctionType(
                    stack[-2],
                    context,
                    stack[-1],
                    closure=stack[-3]
                )
                del stack[-3:]
            else:
                function = types.FunctionType(
                    stack[-2],
                    context,
                    stack[-1]
                )
                del stack[-2:]

            if arg & 4:
                # notice: annotation is not used
                stack.pop()

            if arg & 2:
                function.__kwdefaults__ = stack.pop()

            if arg & 1:
                function.__defaults__ = stack.pop()

            stack.append(function)
        elif opname == 'BUILD_SLICE':
            stack[len(stack) - arg:] = slice(stack[len(stack) - arg:]),
        elif opname == 'EXTENDED_ARG':
            raise exception.DisError(opname)
        elif opname == 'FORMAT_VALUE':
            if arg & 4:
                spec = stack[-1]
                stack.pop()
            else:
                spec = ''

            if arg & 3 == 0:
                stack[-1] = format(stack[-1], spec)
            elif arg & 3 == 1:
                stack[-1] = format(str(stack[-1]), spec)
            elif arg & 3 == 2:
                stack[-1] = format(repr(stack[-1]), spec)
            elif arg & 3 == 3:
                stack[-1] = format(ascii(stack[-1]), spec)
        elif opname == 'HAVE_ARGUMENT':
            raise exception.DisError(opname)
        else:
            raise exception.DisError(opname)

        return False

    @functools.wraps(function)
    def build(
            *args: typing.Any,
            **kwargs: typing.Any
    ) -> typing.Any:
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()

        global_dict: typing.Dict[str, typing.Any] = {
            'with_': ast.Initial('with'),
            'select': ast.Initial('select'),
            'select_distinct': ast.Initial('select_distinct'),
            'insert_into': ast.Initial('insert_into'),
        }

        local_dict: typing.Dict[str, typing.Any] = {
            **bound_arguments.arguments,
        }

        cells: typing.Tuple[types.CellType, ...] = (
            *(function.__closure__ or ()),
            *(
                # TODO: types.CellType in python 3.8
                (lambda x: lambda: x)(None).__closure__[0]
                for _ in function.__code__.co_cellvars or ()
            ),
        )

        stack: typing.List[typing.Any] = []

        # notice: see dis.opmap
        for instruction in instructions:
            done = run(
                global_dict,
                local_dict,
                cells,
                stack,
                instruction.opname,
                instruction.arg,
                instruction.argval
            )

            if done:
                assert len(stack) == 1

                return stack.pop()

    return build
