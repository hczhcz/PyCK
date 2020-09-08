import dis
import functools
import inspect
import types
import typing

from ck import exception
from ck.query import ast


def _run(
        global_dict: typing.Dict[str, typing.Any],
        local_dict: typing.Dict[str, typing.Any],
        # TODO: use types.CellType
        cells: typing.Tuple[typing.Any, ...],
        stack: typing.List[typing.Any],
        opname: str,
        # TODO: int?
        arg: typing.Any,
        argval: typing.Any
) -> bool:
    # TODO
    # pylint: disable=trailing-comma-tuple

    def call_named(
            name: str,
            *args: typing.Any
    ) -> ast.BaseAST:
        return ast.Call(ast.Raw(name), *args)

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
        stack[-1] = call_named('negate', call_named('negate', stack[-1]))
    elif opname == 'UNARY_NEGATIVE':
        stack[-1] = call_named('negate', stack[-1])
    elif opname == 'UNARY_NOT':
        stack[-1] = call_named('not', stack[-1])
    elif opname == 'UNARY_INVERT':
        stack[-1] = call_named('bitNot', stack[-1])
    elif opname == 'GET_ITER':
        stack[-1] = iter(stack[-1])
    elif opname == 'GET_YIELD_FROM_ITER':
        # TODO: more accurate semantic
        stack[-1] = iter(stack[-1])
    elif opname == 'BINARY_POWER':
        stack[-2:] = call_named('pow', *stack[-2:]),
    elif opname == 'BINARY_MULTIPLY':
        stack[-2:] = call_named('multiply', *stack[-2:]),
    elif opname == 'BINARY_MATRIX_MULTIPLY':
        stack[-2:] = call_named('cast', *stack[-2:]),
    elif opname == 'BINARY_FLOOR_DIVIDE':
        stack[-2:] = call_named('intDiv', *stack[-2:]),
    elif opname == 'BINARY_TRUE_DIVIDE':
        stack[-2:] = call_named('divide', *stack[-2:]),
    elif opname == 'BINARY_MODULO':
        stack[-2:] = call_named('modulo', *stack[-2:]),
    elif opname == 'BINARY_ADD':
        stack[-2:] = call_named('plus', *stack[-2:]),
    elif opname == 'BINARY_SUBTRACT':
        stack[-2:] = call_named('minus', *stack[-2:]),
    elif opname == 'BINARY_SUBSCR':
        # TODO: subscr for slices?
        # TODO: general element access for array, tuple, and string?
        stack[-2:] = call_named('arrayElement', *stack[-2:]),
    elif opname == 'BINARY_LSHIFT':
        stack[-2:] = call_named('bitShiftLeft', *stack[-2:]),
    elif opname == 'BINARY_RSHIFT':
        stack[-2:] = call_named('bitShiftRight', *stack[-2:]),
    elif opname == 'BINARY_AND':
        stack[-2:] = call_named('bitAnd', *stack[-2:]),
    elif opname == 'BINARY_XOR':
        stack[-2:] = call_named('bitXor', *stack[-2:]),
    elif opname == 'BINARY_OR':
        stack[-2:] = call_named('bitOr', *stack[-2:]),
    elif opname == 'INPLACE_POWER':
        stack[-2:] = call_named('pow', *stack[-2:]),
    elif opname == 'INPLACE_MULTIPLY':
        stack[-2:] = call_named('multiply', *stack[-2:]),
    elif opname == 'INPLACE_MATRIX_MULTIPLY':
        stack[-2:] = call_named('cast', *stack[-2:]),
    elif opname == 'INPLACE_FLOOR_DIVIDE':
        stack[-2:] = call_named('intDiv', *stack[-2:]),
    elif opname == 'INPLACE_TRUE_DIVIDE':
        stack[-2:] = call_named('divide', *stack[-2:]),
    elif opname == 'INPLACE_MODULO':
        stack[-2:] = call_named('modulo', *stack[-2:]),
    elif opname == 'INPLACE_ADD':
        stack[-2:] = call_named('plus', *stack[-2:]),
    elif opname == 'INPLACE_SUBTRACT':
        stack[-2:] = call_named('minus', *stack[-2:]),
    elif opname == 'INPLACE_LSHIFT':
        stack[-2:] = call_named('bitShiftLeft', *stack[-2:]),
    elif opname == 'INPLACE_RSHIFT':
        stack[-2:] = call_named('bitShiftRight', *stack[-2:]),
    elif opname == 'INPLACE_AND':
        stack[-2:] = call_named('bitAnd', *stack[-2:]),
    elif opname == 'INPLACE_XOR':
        stack[-2:] = call_named('bitXor', *stack[-2:]),
    elif opname == 'INPLACE_OR':
        stack[-2:] = call_named('bitOr', *stack[-2:]),
    elif opname == 'STORE_SUBSCR':
        stack[-3:] = call_named(
            'arrayConcat',
            call_named(
                'arraySlice',
                stack[-2],
                1,
                call_named('minus', stack[-1], 1)
            ),
            call_named('array', stack[-3]),
            call_named(
                'arraySlice',
                stack[-2],
                call_named('plus', stack[-1], 1)
            )
        ),
    elif opname == 'DELETE_SUBSCR':
        stack[-2:] = call_named(
            'arrayConcat',
            call_named(
                'arraySlice',
                stack[-2],
                1,
                call_named('minus', stack[-1], 1)
            ),
            call_named(
                'arraySlice',
                stack[-2],
                call_named('plus', stack[-1], 1)
            )
        ),
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
        stack.append(__build_class__)  # type: ignore[name-defined]
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
        lo = arg % 256  # pylint: disable=invalid-name
        hi = arg // 256  # pylint: disable=invalid-name

        if hi:
            stack[-1:] = *stack[-1][:lo], stack[-1][lo:-hi], *stack[-1][-hi:]
        else:
            stack[-1:] = *stack[-1][:lo], stack[-1][lo:]
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
            stack[-1] = call_named('tupleElement', stack[-1], argval)
    elif opname == 'COMPARE_OP':
        # notice: see dis.cmp_op
        if argval == '<':
            stack[-2:] = call_named('less', *stack[-2:]),
        elif argval == '<=':
            stack[-2:] = call_named('lessOrEquals', *stack[-2:]),
        elif argval == '==':
            stack[-2:] = call_named('equals', *stack[-2:]),
        elif argval == '!=':
            stack[-2:] = call_named('notEquals', *stack[-2:]),
        elif argval == '>':
            stack[-2:] = call_named('greater', *stack[-2:]),
        elif argval == '>=':
            stack[-2:] = call_named('greaterOrEquals', *stack[-2:]),
        elif argval == 'in':
            stack[-2:] = call_named('in', *stack[-2:]),
        elif argval == 'not in':
            stack[-2:] = call_named('notIn', *stack[-2:]),
        elif argval == 'is':
            stack[-2:] = call_named(
                'and',
                call_named(
                    'equals',
                    call_named('toTypeName', stack[-2]),
                    call_named('toTypeName', stack[-1])
                ),
                call_named('equals', *stack[-2:])
            ),
        elif argval == 'is not':
            stack[-2:] = call_named(
                'or',
                call_named(
                    'notEquals',
                    call_named('toTypeName', stack[-2]),
                    call_named('toTypeName', stack[-1])
                ),
                call_named('notEquals', *stack[-2:])
            ),
        elif argval == 'exception match':
            raise exception.DisError(opname)
        elif argval == 'BAD':
            raise exception.DisError(opname)
        else:
            raise exception.DisError(opname)
    elif opname == 'IMPORT_NAME':
        stack[-2:] = __import__(argval, fromlist=stack[-2], level=stack[-1]),
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
        stack.append(local_dict[argval])
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
                *stack[len(stack) - arg:]
            ),
        elif isinstance(stack[-arg - 1], ast.Call):
            stack[-arg - 1:] = ast.Call(
                stack[-arg - 1],
                *stack[len(stack) - arg:]
            ),
        elif isinstance(stack[-arg - 1], ast.BaseStatement):
            stack[-arg - 1:] = ast.ListClause(
                stack[-arg - 1],
                *stack[len(stack) - arg:]
            ),
        else:
            stack[-arg - 1:] = stack[-arg - 1](*stack[len(stack) - arg:]),
    elif opname == 'CALL_FUNCTION_KW':
        if isinstance(stack[-arg - 2], ast.Identifier):
            if stack[-1]:
                raise TypeError()

            stack[-arg - 2:] = ast.Call(stack[-arg - 2], *stack[-arg - 1:-1]),
        elif isinstance(stack[-arg - 2], ast.Call):
            if stack[-1]:
                raise TypeError()

            stack[-arg - 2:] = ast.Call(stack[-arg - 2], *stack[-arg - 1:-1]),
        elif isinstance(stack[-arg - 2], ast.BaseStatement):
            stack[-arg - 2:] = ast.ListClause(
                stack[-arg - 2],
                *stack[-arg - 1:-len(stack[-1]) - 1],
                **dict(zip(stack[-1], stack[-len(stack[-1]) - 1:-1]))
            ),
        else:
            stack[-arg - 2:] = stack[-arg - 2](
                *stack[-arg - 1:-len(stack[-1]) - 1],
                **dict(zip(stack[-1], stack[-len(stack[-1]) - 1:-1]))
            ),
    elif opname == 'CALL_FUNCTION_EX':
        if arg & 1:
            kwargs = stack[-1]
            stack.pop()
        else:
            kwargs = {}

        if isinstance(stack[-2], ast.Identifier):
            if kwargs:
                raise TypeError()

            stack[-2:] = ast.Call(stack[-2], *stack[-1]),
        elif isinstance(stack[-2], ast.Call):
            if kwargs:
                raise TypeError()

            stack[-2:] = ast.Call(stack[-2], *stack[-1]),
        elif isinstance(stack[-2], ast.BaseStatement):
            stack[-2:] = ast.ListClause(stack[-2], *stack[-1], **kwargs),
        else:
            stack[-2:] = stack[-2](*stack[-1], **kwargs),
    elif opname == 'LOAD_METHOD':
        if isinstance(stack[-1], ast.BaseStatement):
            stack[-1:] = ast.SimpleClause(stack[-1], argval), stack[-1]
        else:
            stack[-1:] = getattr(stack[-1], argval).__func__, stack[-1]
    elif opname == 'CALL_METHOD':
        if isinstance(stack[-arg - 2], ast.BaseStatement):
            stack[-arg - 2:] = ast.ListClause(
                stack[-arg - 2],
                *stack[len(stack) - arg:]
            ),
        else:
            stack[-arg - 2:] = stack[-arg - 2](*stack[-arg - 1:]),
    elif opname == 'MAKE_FUNCTION':
        # TODO
        if arg & 8:
            function = sql_template(
                types.FunctionType(
                    stack[-2],
                    global_dict,
                    stack[-1],
                    closure=stack[-3]
                )
            )
            del stack[-3:]
        else:
            function = sql_template(
                types.FunctionType(stack[-2], global_dict, stack[-1])
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


def sql_template(
        function: types.FunctionType
) -> types.FunctionType:
    signature = inspect.signature(function)
    instructions = list(dis.get_instructions(function))

    @functools.wraps(function)
    def build(
            *args: typing.Any,
            **kwargs: typing.Any
    ) -> typing.Any:
        bound_arguments = signature.bind(*args, **kwargs)
        bound_arguments.apply_defaults()

        global_dict: typing.Dict[str, typing.Any] = {
            # supported queries:
            #     with ... select ...
            #     select ...
            #     insert into ... select ...
            #     create table ... engine = ... as select ...
            #     create view ... as select ...
            #     create materialized view ... as select ...
            'with_': ast.Initial('with'),
            'select': ast.Initial('select'),
            'select_distinct': ast.Initial('select_distinct'),
            'insert': ast.Initial('insert'),
            'insert_into': ast.Initial('insert_into'),
            'create': ast.Initial('create'),
            'create_table': ast.Initial('create_table'),
            'create_table_if_not_exists':
                ast.Initial('create_table_if_not_exists'),
            'create_view': ast.Initial('create_view'),
            'create_or_replace_view': ast.Initial('create_or_replace_view'),
            'create_view_if_not_exists':
                ast.Initial('create_view_if_not_exists'),
            'create_materialized_view':
                ast.Initial('create_materialized_view'),
            'create_materialized_view_if_not_exists':
                ast.Initial('create_materialized_view_if_not_exists'),
        }

        local_dict: typing.Dict[str, typing.Any] = {
            **bound_arguments.arguments,
        }

        # TODO: use types.CellType in type annotation
        cells: typing.Tuple[typing.Any, ...] = (
            *(function.__closure__ or ()),
            *(
                types.CellType()  # type: ignore[attr-defined]
                for _ in function.__code__.co_cellvars or ()
            ),
        )

        stack: typing.List[typing.Any] = []

        # notice: see dis.opmap
        for instruction in instructions:
            done = _run(
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

        return None

    # TODO
    return typing.cast(types.FunctionType, build)


def sql_render(
        function: types.FunctionType,
        *args: typing.Any,
        **kwargs: typing.Any
) -> str:
    result = sql_template(function)(*args, **kwargs)

    if isinstance(result, ast.BaseAST):
        return result.render_statement()

    return ast.Value(result).render_statement()
