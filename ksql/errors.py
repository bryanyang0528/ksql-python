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
