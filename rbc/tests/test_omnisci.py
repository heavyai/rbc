import os

import pytest
rbc_mapd = pytest.importorskip('rbc.mapd')


def test_get_client_config(tmpdir):
    d = tmpdir.mkdir("omnisci")
    fh = d.join("client.conf")
    fh.write("""
    [user]
name  =  foo
password = secret

[server]
port: 1234
host: example.com

[rbc]
debug: False
use_host_target: False
# server: Server [NOT IMPL]
# target_info: TargetInfo
""")
    conf_file = os.path.join(fh.dirname, fh.basename)

    old_conf = os.environ.get('OMNISCI_CLIENT_CONF')
    os.environ['OMNISCI_CLIENT_CONF'] = conf_file

    try:
        conf = rbc_mapd.get_client_config()
        assert conf['user'] == 'foo'
        assert conf['password'] == 'secret'
        assert conf['port'] == 1234
        assert conf['host'] == 'example.com'
        assert conf['dbname'] == 'omnisci'
        assert conf['debug'] == bool(0)
        conf = rbc_mapd.get_client_config(dbname='test')
        assert conf['dbname'] == 'test'
    finally:
        if old_conf is None:
            del os.environ['OMNISCI_CLIENT_CONF']
        else:
            os.environ['OMNISCI_CLIENT_CONF'] = old_conf
