class QueryError(RuntimeError):
    pass


class ShellError(RuntimeError):
    pass


class ServiceError(RuntimeError):
    pass


class DisError(SyntaxError):
    pass
