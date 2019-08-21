class DDL(object):
    def __init__(self, schemas=[]):
        self.__schemas = schemas

    def create_statements(self):
        return map(lambda schema: schema.get_local_table_definition(), self.__schemas)

    def drop_statements(self):
        return map(lambda schema: schema.get_local_drop_table_statement(), self.__schemas)
