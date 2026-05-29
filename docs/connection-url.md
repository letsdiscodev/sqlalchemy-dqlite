# Connection URL

```
dqlite://host:port/database
dqlite+aio://host:port/database     # async engine
```

```python
from sqlalchemy import create_engine
engine = create_engine("dqlite://localhost:9001/mydb")
```

## The host:port is a bootstrap address

The host:port pair is just the *bootstrap* node. The dqlite client connects
there, reads that node's leader-info response, and discovers the rest of the
cluster from it. You point at one reachable node, not the whole cluster.

If that host is unreachable, leader discovery cannot start. Multi-address
bootstrap is not exposed at the dialect URL surface — if you want
bootstrap-from-many, put a load balancer or DNS round-robin in front of the
cluster, or rotate the URL host across deployments.

## Query parameters

Connection options can be passed as URL query parameters (e.g.
`max_total_rows`). When a parameter is repeated
(`?max_total_rows=100&max_total_rows=200`), the **last** occurrence wins —
matching `urllib.parse.parse_qsl` ordering. Templated URLs that layer values
from multiple sources should be aware that a duplicated key silently
overrides the earlier value rather than raising.
