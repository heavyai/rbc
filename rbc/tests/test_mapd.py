
from rbc.mapd import RemoteMapD


def test_get_version():
    mapd = RemoteMapD()

    @mapd('f64(f64, f64)', 'i64(i64,i64)')
    def add(x, y):
        return x + y

    print(add.get_MapD_version())
    descr, r = add.sql_execute('select dest_merc_x, dest_merc_y from flights_2008_10k limit 10')
    print(descr)
    print(list(r))

    print(add.register())
