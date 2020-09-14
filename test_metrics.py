from urllib3.connectionpool import HTTPConnectionPool
import simplejson as json
from datasketches import kll_floats_sketch

pool = HTTPConnectionPool("localhost", 8123)
response = pool.urlopen(
    "POST",
    "/",
    headers={"Connection": "keep-alive", "Accept-Encoding": "gzip,deflate"},
    body="SELECT * FROM metrics_local FORMAT JSON;",
    chunked=True,
)

data = json.loads(response.data)
kll = kll_floats_sketch(8)
for row in data["data"]:
    print(row["timestamp"])
    quantiles = row["quantiles_sketch"]
    quant_bytes = bytearray()
    for i in quantiles:
        quant_bytes.append(i)
    row_quant = kll_floats_sketch.deserialize(bytes(quant_bytes))
    kll.merge(row_quant)

print(kll.get_quantile(0.5))
