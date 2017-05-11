# zuora_restful_python
A python class for accessing the Zuora API via REST

# install
```
pip install  git+git://github.com/bolaurent/zuora_restful_python.git
```

# sample use

```
import sys
from zuora_restful_python.zuora import Zuora

(username, password) = sys.argv[1:3]
zuora = Zuora(username, password)

for record in zuora.query_all('select Name from Account'):
    print(record['Name'])
```

## References

* [Zuora Developer Center](https://www.zuora.com/developer/)

## Note

if you are looking for the command line tool zoql, it has moved to https://github.com/bolaurent/python-cmdline-zoql.
