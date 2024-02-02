#!/usr/bin/python3

import json
import requests
from schema_registry import *

auth = requests.auth.HTTPBasicAuth('someone', 'something')
ca_cert = '/db/cert.pem'
cli = SchemaRegistryClient(['https://localhost:8080'], auth=auth, ca_cert=ca_cert)

m = cli.list_versions(name = 'madeup')
_id = m[0]
s1 = cli.get_schema(_id = _id)

obj = json.load(open('/db/schema2.json'))
s2 = Schema.from_dict(obj)
