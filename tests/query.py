from ck import query


def test_query_escape_text() -> None:
    assert query.ast.escape_text('\x00\\\n', '\'') == '\'\\0\\\\\\n\''
    assert query.ast.escape_text('test!', '!') == '!test\\!!'


def test_query_escape_buffer() -> None:
    assert query.ast.escape_buffer(b'\x00\\\xff', '\'') == '\'\\0\\\\\\xff\''
    assert query.ast.escape_buffer(b'test!', '!') == '!test\\!!'


def test_query_escape_value() -> None:
    assert query.ast.escape_value(None) == 'null'
    assert query.ast.escape_value(Ellipsis) == '*'
    assert query.ast.escape_value(False) == 'false'
    assert query.ast.escape_value(True) == 'true'
    assert query.ast.escape_value(123) == '123'
    assert query.ast.escape_value(123.) == '123.0'
    assert query.ast.escape_value(123e456) == 'inf'
    assert query.ast.escape_value(1 + 2j) == 'tuple(1.0, 2.0)'
    assert query.ast.escape_value([]) == 'array()'
    assert query.ast.escape_value([1, 2, 3]) == 'array(1, 2, 3)'
    assert query.ast.escape_value(tuple()) == 'tuple()'
    assert query.ast.escape_value((1, 2, 3)) == 'tuple(1, 2, 3)'
    assert query.ast.escape_value(range(123)) == 'range(0, 123, 1)'
    assert query.ast.escape_value(range(1, 2, 3)) == 'range(1, 2, 3)'
    assert query.ast.escape_value('test\n') == '\'test\\n\''
    assert query.ast.escape_value(b'test\xff') == '\'test\\xff\''
    assert query.ast.escape_value(bytearray(b'test\xff')) == '\'test\\xff\''
    assert query.ast.escape_value(memoryview(b'test\xff')) == '\'test\\xff\''
    assert query.ast.escape_value(set()) == 'array()'
    assert query.ast.escape_value({1, 2, 3}) == 'array(1, 2, 3)'
    assert query.ast.escape_value(frozenset()) == 'array()'
    assert query.ast.escape_value(frozenset({1, 2, 3})) == 'array(1, 2, 3)'
    assert query.ast.escape_value({}) == 'array()'
    assert query.ast.escape_value({1: 2}) == 'array(tuple(1, 2))'


def test_query_identifier() -> None:
    raw = query.ast.Raw('test')

    assert raw.render_expression() == 'test'
    assert raw.render_statement() == 'test'


def test_query_identifier() -> None:
    identifier_1 = query.ast.Identifier('test')
    identifier_2 = query.ast.Identifier('test`')

    assert identifier_1.render_expression() == '`test`'
    assert identifier_1.render_statement() == 'select `test`'
    assert identifier_2.render_expression() == '`test\\``'
    assert identifier_2.render_statement() == 'select `test\\``'


def test_query_call() -> None:
    identifier = query.ast.Identifier('test')
    call_1 = query.ast.Call(identifier, [])
    call_2 = query.ast.Call(call_1, [1, call_1])

    assert call_1.render_expression() == '`test`()'
    assert call_1.render_statement() == 'select `test`()'
    assert call_2.render_expression() == '`test`()(1, `test`())'
    assert call_2.render_statement() == 'select `test`()(1, `test`())'


def test_query_initial() -> None:
    initial_1 = query.ast.Initial('test')
    initial_2 = query.ast.Initial('__test__test__')

    assert initial_1.render_expression() == '(test)'
    assert initial_1.render_statement() == 'test'
    assert initial_2.render_expression() == '(test test)'
    assert initial_2.render_statement() == 'test test'


def test_query_simple_clause() -> None:
    initial = query.ast.Initial('select')
    clause_1 = query.ast.SimpleClause(initial, 'test')
    clause_2 = query.ast.SimpleClause(initial, '__test__test__')

    assert clause_1.render_expression() == '(select test)'
    assert clause_1.render_statement() == 'select test'
    assert clause_2.render_expression() == '(select test test)'
    assert clause_2.render_statement() == 'select test test'


def test_query_list_clause() -> None:
    initial = query.ast.Initial('select')
    clause_1 = query.ast.ListClause(initial, [], {})
    clause_2 = query.ast.ListClause(initial, [1, clause_1], {'test': 2})

    assert clause_1.render_expression() == '(select)'
    assert clause_1.render_statement() == 'select'
    assert clause_2.render_expression() == '(select 1, (select), 2 as `test`)'
    assert clause_2.render_statement() == 'select 1, (select), 2 as `test`'
