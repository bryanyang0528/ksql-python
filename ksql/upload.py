import os

from ksql.client import KSQLAPI
from ksql.errors import FileTypeError, InvalidQueryError


class FileUpload(object):
    """ UPLOAD KSQL RULES AS FILES """

    def __init__(self, url, **kwargs):
        """ Instantiate the url pointer and the client object """
        self.url = url
        self.client = KSQLAPI(url)

    def upload(self, ksqlfile):
        """
        A method to upload ksql rules as .ksql file

        Parameter List
        -------------
        :param ksqlfile: File containing the ksql rules to be uploaded.
                          Only supports ksql queries and not streaming queries

        """

        # check if the file is .ksql
        self.checkExtension(ksqlfile)

        # parse the file and get back the rules
        rules = self.get_rules_list(ksqlfile)
        log_return = []
        for each_rule in rules:
            resp = self.client.ksql(each_rule)
            log_return.append(resp)

        return log_return

    def get_rules_list(self, ksqlfile):
        with open(ksqlfile) as rf:
            rule = ""

            for line in rf:
                rule = rule + " " + line.strip()

                if rule[-1:] == ";":
                    yield rule
                    rule = ""

            if rule[-1:] != ";":
                raise InvalidQueryError(rule)

    def checkExtension(self, filename):
        ext = os.path.splitext(filename)[-1].lower()

        if ext != ".ksql":
            raise FileTypeError(ext)
        return
