
(heavydb-connect)=
## Connect to the HeavyDB server

```python
from rbc.heavydb import RemoteHeavyDB
heavydb = RemoteHeavyDB(user='admin', password='HyperInteractive',
                        host='127.0.0.1', port=6274)
```