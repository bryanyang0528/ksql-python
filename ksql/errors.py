class SQLTypeNotImplementYetError(Exception):
    def __init__(self, sql_type):
        self.msg = "This type {} has not be implement yet.".format(sql_type)


class IllegalTableTypeError(Exception):
    def __init__(self, table_type):
        self.msg = "This table type {} is illegal.".format(table_type)


class IllegalValueFormatError(Exception):
    def __init__(self, value_format):
        self.msg = "This value format {} is illegal.".format(value_format)


class CreateError(Exception):
    def __init__(self, e):
        self.msg = "{}".format(e)


class SQLFormatNotImplementError(Exception):
    pass


class BuildNotImplmentError(Exception):
    pass


class FileTypeError(Exception):
    def __init__(self, ext):
        self.msg = "This {} file extension is not valid".format(ext)


class InvalidQueryError(Exception):
    def __init__(self, query):
        self.msg = "The query:\n{}\n is invalid".format(query)


class KSQLError(Exception):
    def __init__(self, e, error_code=None, stackTrace=None):
        self.msg = "{}".format(e)
        self.error_code = error_code
        self.stackTrace = stackTrace
