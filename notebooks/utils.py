from rbc.tests import test_classes


def load_test_data(heavydb, test_cls: str, table_name: str):
    for cls in test_classes:
        if cls == test_cls:
            obj = cls()
            heavydb.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
            heavydb.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({obj.table_defn})')
            heavydb.load_table_columnar(f'{table_name}', **obj.values)
