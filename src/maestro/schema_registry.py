#!/usr/bin/python3

import os
import sys
import threading
import pdb
import socket
import logging
import etcd3
import json
import enum
import hashlib
import requests
from ovis_ldms import ldms

from functools import wraps, cached_property

from flask import Flask, Blueprint, current_app, g, request, Response
from gevent.pywsgi import WSGIServer

# Module doc
__doc__ = r"""
schema_registry


SchemaRegistry (the server)
---------------------------

The `SchemaRegistry` object contains a routine to serve LDMS Schema Registry
Service over HTTP (or HTTPS). The application can deploy the service as follows:

>>> svc = SchemaRegistry( etcd_spec )
>>> svc.start()

The `etcd_spec` is a `dict` containing `etcd` configuration with extra
`"schema_registry"` section. The following is an example of a `schema_registry`
yaml configuration.

```
schema_registry
  etcd_prefix: "/headnode/schema-registry"
  listen: "*:8080"
  keyfile: "/db/key.pem"
  certfile: "/db/cert.pem"
  auth:
    type: simple
    users:
     - username: someone
       password: something
     - username: anotherone
       password: anotherthing
```

Description for each option:
- `etcd_prefix` is the prefix in ETCD database to store Schema Reigstry data
- `listen` is an "_ADDRESS_:_PORT_" to listen and serve HTTP (or HTTPS) Schema
  Registry Service.
- `keyfile` is the private SSL/TLS key for HTTPS. If this option is specified,
  the service is served over HTTPS.
- `certfile` is the SSL/TLS certificate (public) for HTTPS. This is required to
  serve over hTTPS.
- `auth` is the authentication option section, which can specify `type` for the
  type of the authentication method. Different authentication method may require
  different additional parameters.
  - `type` currently we only support "simple" (Simple HTTP) authentication, in
    which `users` can be defined as a list of username-password dictionary as
    shown in the example above.


SchemaRegistryClient (the client)
---------------------------------

The client application can use SchemaRegistryClient as follows:
>>> urls = [ "https://someone:something@host1:8080",
...          "https://someone:something@host2:8080" ] # list of 1 URL is OK too
>>> cli = SchemaRegistryClient(urls, ca_cert = "/db/cert.pem")

The multiple entries of `urls` is for availability. If the current server became
unavailable (e.g. down), the client will internally try the next one in a
round-robin fashion. If all servers are not available, an exception will be
raised.

The `ca_cert` option is useful for self-signed certificate of the Schema
Registry Server. If the certificate is signed by a Trusted Certificate Authority
(e.g. signed by a CA in /etc/ssl/certs/), the `ca_cert` is not required.


Schema JSON Format
------------------

The format is inspired by Avro Schema JSON format
(https://avro.apache.org/docs/1.11.1/specification/),
with LDMS-specific twists. The client uses the following format to submit (PUT)
schema definition to the schema registry.

```json
  {
    "name": _SCHEMA_NAME_,
    "type": _TYPE_NAME_ , // usually "record"
    "fields": [ // describing the "record"
      { "name": _METRIC_NAME_, "type": _METRIC_TYPE_ },
      ...
    ]
  }
```

The PUT request shall encapsulate the schema inside a JSON document under the
"schema" attribute. For example,

```json
  {
    "schema": {
      "name" : _SCHEMA_NAME_,
      ...
    }
  }
```
or,
```json
  {
    "schema": "{ \"name\": \"_SCHEMA_NAME_\", ... }"
    // JSON object encoded in string
  }
```


"""

log = logging.getLogger(__name__)

class SchemaReigstryDebug(object):
    pass

_SD = SchemaReigstryDebug()

#------------------------#
#                        #
# blueprint for WSGI app #
#                        #
#------------------------#

blueprint = Blueprint('root', __name__,  url_prefix='')
_SD.blueprint = blueprint
_SD.name = __name__

def json_resp(content):
    return Response(content, mimetype="application/json")

@blueprint.errorhandler(Exception)
def error_handler(e):
    if sys.flags.interactive:
        pdb.post_mortem(e.__traceback__)

@blueprint.before_request
def auth_check():
    sr = current_app.sr
    if sr.auth:
        failed_reason = sr.auth.authorize()
        if failed_reason:
            return failed_reason
    return None

@blueprint.get('/')
def index_GET():
    return f'index'

@blueprint.post('/')
def index_POST():
    sr = current_app.sr
    obj = request.json # The JSON object in the PUT request
    if not obj:
        return f'Missing input JSON object', 500
    s = Schema.from_dict(obj)
    sr.add_schema(s)
    return json_resp(json.dumps({"id": s.id}))

URL_SCHEMAS_IDS = '/schemas/ids/<_id>'
@blueprint.get(URL_SCHEMAS_IDS)
def schemas_ids_id_GET(_id):
    sr = current_app.sr
    sch = sr.get_schema(_id)
    ret = sch.as_json()
    return json_resp(ret)

@blueprint.delete(URL_SCHEMAS_IDS)
def schemas_ids_id_DEL(_id):
    sr = current_app.sr
    sr.delete_schema(_id = _id)
    return json_resp(json.dumps([ _id ]))

URL_NAMES_DIR = '/names'
@blueprint.get(URL_NAMES_DIR)
@blueprint.get('/subjects')
def names_GET():
    # GET to list schemas
    # PUT to add a (potentially new) schema into etcd. The SHA of the schema is
    #     used as schema ID, and schema 'name'
    #
    # schema object in etcd will have the following attributes:
    #   - name
    #   - id (sha)
    #   - create_timestamp
    #   - metrics: (array)
    #     - name
    #     - type
    sr = current_app.sr
    objs = sr.list_names()
    ret = [ s for s in objs.keys() ]
    return json_resp(json.dumps(ret))

URL_NAMES = '/names/<name>'
@blueprint.delete(URL_NAMES)
def names_name_DEL(name):
    sr = current_app.sr
    objs = sr.list_names(name = name)
    ids  = objs.get(name, [])
    del_ids = list()
    for _id in ids:
        sr.delete_schema(_id = _id)
        del_ids.append(_id)
    ret = json.dumps(del_ids)
    return json_resp(ret)

URL_NAMES_VERSIONS_DIR = '/names/<name>/versions'
@blueprint.get(URL_NAMES_VERSIONS_DIR)
@blueprint.get('/subjects/<name>/versions')
def names_name_versions_GET(name):
    sr = current_app.sr
    objs = sr.list_names(name = name)
    ids  = objs.get(name)
    return json_resp(json.dumps(ids))

@blueprint.post(URL_NAMES_VERSIONS_DIR)
@blueprint.post('/subjects/<name>/versions')
def names_name_versions_POST(name):
    sr = current_app.sr
    obj = request.json # The JSON object in the PUT request
    if not obj:
        return f'Missing input JSON object', 500
    s = Schema.from_dict(obj)
    if s.name != name:
        return f"Schema name ('{s.name}') does not match the name in the URL '({name})'", 500
    sr.add_schema(s)
    return json_resp(json.dumps({"id": s.id}))

URL_DIGESTS_DIR = '/digests'
@blueprint.get(URL_DIGESTS_DIR)
def digests_GET():
    # List by digests
    sr = current_app.sr
    objs = sr.list_digests()
    ret = [ s for s in objs.keys() ]
    return json_resp(json.dumps(ret))

URL_DIGESTS_VERSIONS_DIR = '/digests/<digest>/versions'
@blueprint.get(URL_DIGESTS_VERSIONS_DIR)
def digests_digest_versions_GET(digest = ""):
    digest = digest.lower()
    sr = current_app.sr
    objs = sr.list_digests(digest = digest)
    ids  = objs.get(digest)
    return json_resp(json.dumps(ids))

# ------------ end blueprint ------------ #

def srv_proc(app, listen, keyfile, certfile):
    print("srv_proc BEGIN")
    _SD.app = app
    _SD.listen = listen
    if keyfile and certfile:
        srv = WSGIServer(listen, app, keyfile=keyfile, certfile=certfile)
    else:
        srv = WSGIServer(listen, app)
    srv.serve_forever()
    print("srv_proc END")

class SimpleAuth(object):
    def __init__(self, users):
        if not users:
            raise RuntimeError(f'No `users` were given')
        _users = list()
        for o in users:
            if type(o) is dict:
                # expecting {'username': _USERNAME_, 'password': _PASSWORD_ }
                x = (o['username'], o['password'])
            elif type(o) is list:
                # expecting [ _USERNAME_, _PASSWORD_ ]
                x = (o[0], o[1])
            else:
                # bad format
                raise ValueError(f'Unknown username/password format: {o}')
            _users.append(x)
        self.users = set(_users)

    def authorize(self):
        # request is a special flask object referring to the current request
        if not request.authorization:
            return "Nope", 401
        uname = request.authorization.get('username')
        passwd = request.authorization.get('password')
        if (uname, passwd) not in self.users:
            return "Bad credentials", 401
        return None # Good

    @classmethod
    def from_spec(cls, spec):
        users = spec.get('users')
        if not users:
            raise RuntimeError(f'No `users` were listed in `auth`')
        return SimpleAuth(users)


AUTH_TBL = {
        'simple': SimpleAuth,
}

def get_auth(auth_conf):
    if not auth_conf:
        return None
    auth_type = auth_conf.get('type')
    if not auth_type:
        msg = f"`type` was not specified in the `auth` section"
        log.error(msg)
        raise RuntimeError(msg)
    auth_cls = AUTH_TBL.get(auth_type)
    if not auth_cls:
        msg = f"`auth.type`: {auth_type} not found"
        log.error(msg)
        raise RuntimeError(msg)
    auth_obj = auth_cls.from_spec(auth_conf)
    return auth_obj

class ValueType(enum.Enum):
    CHAR = 1
    U8   = 2
    S8   = 3
    U16  = 4
    S16  = 5
    U32  = 6
    S32  = 7
    U64  = 8
    S64  = 9
    F32  = 10
    D64  = 11

    CHAR_ARRAY = 12
    U8_ARRAY   = 13
    S8_ARRAY   = 14
    U16_ARRAY  = 15
    S16_ARRAY  = 16
    U32_ARRAY  = 17
    S32_ARRAY  = 18
    U64_ARRAY  = 19
    S64_ARRAY  = 20
    F32_ARRAY  = 21
    D64_ARRAY  = 22

    LIST         = 23
    LIST_ENTRY   = 24
    RECORD_TYPE  = 25
    RECORD_INST  = 26
    RECORD_ARRAY = 27
    TIMESTAMP    = 28

    @classmethod
    def from_json_str(cls, _str):
        return STR_TYPE_TBL.get(_str)

    def to_json_str(self):
        return TYPE_STR_TBL[self]

    @classmethod
    def from_obj(cls, o):
        typ = type(o)
        if typ == ValueType:
            return o
        if typ == str:
            return cls.from_json_str(o)
        return ValueType(o)

PRIM_TBL = {
    # avro type
    "int"    : ValueType.S32,
    "long"   : ValueType.S64,
    "float"  : ValueType.F32,
    "double" : ValueType.D64,

    # ldms types
    "char" : ValueType.CHAR,
    "u8"   : ValueType.U8,
    "s8"   : ValueType.S8,
    "u16"  : ValueType.U16,
    "s16"  : ValueType.S16,
    "u32"  : ValueType.U32,
    "s32"  : ValueType.S32,
    "u64"  : ValueType.U64,
    "s64"  : ValueType.S64,
    "f32"  : ValueType.F32,
    "d64"  : ValueType.D64,
}

STR_TYPE_TBL = {
    # avro type
    "int"    : ValueType.S32,
    "long"   : ValueType.S64,
    "float"  : ValueType.F32,
    "double" : ValueType.D64,

    # ldms types
    "char" : ValueType.CHAR,
    "u8"   : ValueType.U8,
    "s8"   : ValueType.S8,
    "u16"  : ValueType.U16,
    "s16"  : ValueType.S16,
    "u32"  : ValueType.U32,
    "s32"  : ValueType.S32,
    "u64"  : ValueType.U64,
    "s64"  : ValueType.S64,
    "f32"  : ValueType.F32,
    "d64"  : ValueType.D64,

    "char[]" : ValueType.CHAR_ARRAY,
    "u8[]"   : ValueType.U8_ARRAY,
    "s8[]"   : ValueType.S8_ARRAY,
    "u16[]"  : ValueType.U16_ARRAY,
    "s16[]"  : ValueType.S16_ARRAY,
    "u32[]"  : ValueType.U32_ARRAY,
    "s32[]"  : ValueType.S32_ARRAY,
    "u64[]"  : ValueType.U64_ARRAY,
    "s64[]"  : ValueType.S64_ARRAY,
    "f32[]"  : ValueType.F32_ARRAY,
    "d64[]"  : ValueType.D64_ARRAY,

    "record"   : ValueType.RECORD_TYPE,
    "record[]" : ValueType.RECORD_ARRAY,

    "list"   : ValueType.LIST,
}

ARRAY_TBL = {
    # avro type
    "int"    : ValueType.S32_ARRAY,
    "long"   : ValueType.S64_ARRAY,
    "float"  : ValueType.F32_ARRAY,
    "double" : ValueType.D64_ARRAY,

    # ldms types
    "char" : ValueType.CHAR_ARRAY,
    "u8"   : ValueType.U8_ARRAY,
    "s8"   : ValueType.S8_ARRAY,
    "u16"  : ValueType.U16_ARRAY,
    "s16"  : ValueType.S16_ARRAY,
    "u32"  : ValueType.U32_ARRAY,
    "s32"  : ValueType.S32_ARRAY,
    "u64"  : ValueType.U64_ARRAY,
    "s64"  : ValueType.S64_ARRAY,
    "f32"  : ValueType.F32_ARRAY,
    "d64"  : ValueType.D64_ARRAY,

    "char[]" : ValueType.CHAR_ARRAY,
    "u8[]"   : ValueType.U8_ARRAY,
    "s8[]"   : ValueType.S8_ARRAY,
    "u16[]"  : ValueType.U16_ARRAY,
    "s16[]"  : ValueType.S16_ARRAY,
    "u32[]"  : ValueType.U32_ARRAY,
    "s32[]"  : ValueType.S32_ARRAY,
    "u64[]"  : ValueType.U64_ARRAY,
    "s64[]"  : ValueType.S64_ARRAY,
    "f32[]"  : ValueType.F32_ARRAY,
    "d64[]"  : ValueType.D64_ARRAY,

    "record"   : ValueType.RECORD_ARRAY,
    "record[]" : ValueType.RECORD_ARRAY,

    ValueType.CHAR : ValueType.CHAR_ARRAY,
    ValueType.U8   : ValueType.U8_ARRAY,
    ValueType.S8   : ValueType.S8_ARRAY,
    ValueType.U16  : ValueType.U16_ARRAY,
    ValueType.S16  : ValueType.S16_ARRAY,
    ValueType.U32  : ValueType.U32_ARRAY,
    ValueType.S32  : ValueType.S32_ARRAY,
    ValueType.U64  : ValueType.U64_ARRAY,
    ValueType.S64  : ValueType.S64_ARRAY,
    ValueType.F32  : ValueType.F32_ARRAY,
    ValueType.D64  : ValueType.D64_ARRAY,
    ValueType.RECORD_TYPE  : ValueType.RECORD_ARRAY,

    ValueType.CHAR_ARRAY : ValueType.CHAR_ARRAY,
    ValueType.U8_ARRAY   : ValueType.U8_ARRAY,
    ValueType.S8_ARRAY   : ValueType.S8_ARRAY,
    ValueType.U16_ARRAY  : ValueType.U16_ARRAY,
    ValueType.S16_ARRAY  : ValueType.S16_ARRAY,
    ValueType.U32_ARRAY  : ValueType.U32_ARRAY,
    ValueType.S32_ARRAY  : ValueType.S32_ARRAY,
    ValueType.U64_ARRAY  : ValueType.U64_ARRAY,
    ValueType.S64_ARRAY  : ValueType.S64_ARRAY,
    ValueType.F32_ARRAY  : ValueType.F32_ARRAY,
    ValueType.D64_ARRAY  : ValueType.D64_ARRAY,
    ValueType.RECORD_ARRAY  : ValueType.RECORD_ARRAY,

    ldms.LDMS_V_CHAR : ValueType.CHAR_ARRAY,
    ldms.LDMS_V_U8   : ValueType.U8_ARRAY,
    ldms.LDMS_V_S8   : ValueType.S8_ARRAY,
    ldms.LDMS_V_U16  : ValueType.U16_ARRAY,
    ldms.LDMS_V_S16  : ValueType.S16_ARRAY,
    ldms.LDMS_V_U32  : ValueType.U32_ARRAY,
    ldms.LDMS_V_S32  : ValueType.S32_ARRAY,
    ldms.LDMS_V_U64  : ValueType.U64_ARRAY,
    ldms.LDMS_V_S64  : ValueType.S64_ARRAY,
    ldms.LDMS_V_F32  : ValueType.F32_ARRAY,
    ldms.LDMS_V_D64  : ValueType.D64_ARRAY,
    ldms.LDMS_V_RECORD_TYPE  : ValueType.RECORD_ARRAY,

    ldms.LDMS_V_CHAR_ARRAY : ValueType.CHAR_ARRAY,
    ldms.LDMS_V_U8_ARRAY   : ValueType.U8_ARRAY,
    ldms.LDMS_V_S8_ARRAY   : ValueType.S8_ARRAY,
    ldms.LDMS_V_U16_ARRAY  : ValueType.U16_ARRAY,
    ldms.LDMS_V_S16_ARRAY  : ValueType.S16_ARRAY,
    ldms.LDMS_V_U32_ARRAY  : ValueType.U32_ARRAY,
    ldms.LDMS_V_S32_ARRAY  : ValueType.S32_ARRAY,
    ldms.LDMS_V_U64_ARRAY  : ValueType.U64_ARRAY,
    ldms.LDMS_V_S64_ARRAY  : ValueType.S64_ARRAY,
    ldms.LDMS_V_F32_ARRAY  : ValueType.F32_ARRAY,
    ldms.LDMS_V_D64_ARRAY  : ValueType.D64_ARRAY,
    ldms.LDMS_V_RECORD_ARRAY  : ValueType.RECORD_ARRAY,

}

TYPE_STR_TBL = {
    ValueType.CHAR : "char",
    ValueType.U8   : "u8",
    ValueType.S8   : "s8",
    ValueType.U16  : "u16",
    ValueType.S16  : "s16",
    ValueType.U32  : "u32",
    ValueType.S32  : "int",
    ValueType.U64  : "u64",
    ValueType.S64  : "s64",
    ValueType.F32  : "f32",
    ValueType.D64  : "d64",

    ValueType.CHAR_ARRAY : "char[]",
    ValueType.U8_ARRAY   : "u8[]",
    ValueType.S8_ARRAY   : "s8[]",
    ValueType.U16_ARRAY  : "u16[]",
    ValueType.S16_ARRAY  : "s16[]",
    ValueType.U32_ARRAY  : "u32[]",
    ValueType.S32_ARRAY  : "int[]",
    ValueType.U64_ARRAY  : "u64[]",
    ValueType.S64_ARRAY  : "long[]",
    ValueType.F32_ARRAY  : "float[]",
    ValueType.D64_ARRAY  : "double[]",

    ValueType.LIST  : "list",
    ValueType.RECORD_TYPE  : "record",
    ValueType.RECORD_ARRAY : "record[]",

}

ITEM_TYPE_TBL = {
    # avro type
    "int"    : ValueType.S32,
    "long"   : ValueType.S64,
    "float"  : ValueType.F32,
    "double" : ValueType.D64,

    # ldms types
    "char" : ValueType.CHAR,
    "u8"   : ValueType.U8,
    "s8"   : ValueType.S8,
    "u16"  : ValueType.U16,
    "s16"  : ValueType.S16,
    "u32"  : ValueType.U32,
    "s32"  : ValueType.S32,
    "u64"  : ValueType.U64,
    "s64"  : ValueType.S64,
    "f32"  : ValueType.F32,
    "d64"  : ValueType.D64,

    "char[]" : ValueType.CHAR,
    "u8[]"   : ValueType.U8,
    "s8[]"   : ValueType.S8,
    "u16[]"  : ValueType.U16,
    "s16[]"  : ValueType.S16,
    "u32[]"  : ValueType.U32,
    "s32[]"  : ValueType.S32,
    "u64[]"  : ValueType.U64,
    "s64[]"  : ValueType.S64,
    "f32[]"  : ValueType.F32,
    "d64[]"  : ValueType.D64,

    "record"   : ValueType.RECORD_TYPE,
    "record[]" : ValueType.RECORD_TYPE,

    ValueType.CHAR : ValueType.CHAR,
    ValueType.U8   : ValueType.U8,
    ValueType.S8   : ValueType.S8,
    ValueType.U16  : ValueType.U16,
    ValueType.S16  : ValueType.S16,
    ValueType.U32  : ValueType.U32,
    ValueType.S32  : ValueType.S32,
    ValueType.U64  : ValueType.U64,
    ValueType.S64  : ValueType.S64,
    ValueType.F32  : ValueType.F32,
    ValueType.D64  : ValueType.D64,
    ValueType.RECORD_TYPE  : ValueType.RECORD_TYPE,

    ValueType.CHAR_ARRAY : ValueType.CHAR,
    ValueType.U8_ARRAY   : ValueType.U8,
    ValueType.S8_ARRAY   : ValueType.S8,
    ValueType.U16_ARRAY  : ValueType.U16,
    ValueType.S16_ARRAY  : ValueType.S16,
    ValueType.U32_ARRAY  : ValueType.U32,
    ValueType.S32_ARRAY  : ValueType.S32,
    ValueType.U64_ARRAY  : ValueType.U64,
    ValueType.S64_ARRAY  : ValueType.S64,
    ValueType.F32_ARRAY  : ValueType.F32,
    ValueType.D64_ARRAY  : ValueType.D64,
    ValueType.RECORD_ARRAY  : ValueType.RECORD_TYPE,

    ldms.LDMS_V_CHAR : ValueType.CHAR,
    ldms.LDMS_V_U8   : ValueType.U8,
    ldms.LDMS_V_S8   : ValueType.S8,
    ldms.LDMS_V_U16  : ValueType.U16,
    ldms.LDMS_V_S16  : ValueType.S16,
    ldms.LDMS_V_U32  : ValueType.U32,
    ldms.LDMS_V_S32  : ValueType.S32,
    ldms.LDMS_V_U64  : ValueType.U64,
    ldms.LDMS_V_S64  : ValueType.S64,
    ldms.LDMS_V_F32  : ValueType.F32,
    ldms.LDMS_V_D64  : ValueType.D64,
    ldms.LDMS_V_RECORD_TYPE  : ValueType.RECORD_TYPE,

    ldms.LDMS_V_CHAR_ARRAY : ValueType.CHAR,
    ldms.LDMS_V_U8_ARRAY   : ValueType.U8,
    ldms.LDMS_V_S8_ARRAY   : ValueType.S8,
    ldms.LDMS_V_U16_ARRAY  : ValueType.U16,
    ldms.LDMS_V_S16_ARRAY  : ValueType.S16,
    ldms.LDMS_V_U32_ARRAY  : ValueType.U32,
    ldms.LDMS_V_S32_ARRAY  : ValueType.S32,
    ldms.LDMS_V_U64_ARRAY  : ValueType.U64,
    ldms.LDMS_V_S64_ARRAY  : ValueType.S64,
    ldms.LDMS_V_F32_ARRAY  : ValueType.F32,
    ldms.LDMS_V_D64_ARRAY  : ValueType.D64,
    ldms.LDMS_V_RECORD_ARRAY  : ValueType.RECORD_TYPE,
}

class SchemaMetric(object):
    """Base for all metric definition"""

    def __init__(self, name, _type, doc = None, is_meta = False, units = None,
                 *args, **kwargs):
        assert(_type is not None)
        self.name = name
        self.type = ValueType.from_obj(_type)
        self.doc  = doc
        self.units = units
        self.is_meta = bool(is_meta)

    @classmethod
    def from_dict(cls, obj):
        # Use specific class according to 'type'
        _type = obj['type']
        if _type == "array":
            return SchemaMetricArray.from_dict(obj)
        elif _type == "list":
            return SchemaMetricList.from_dict(obj)
        elif _type == "record":
            return SchemaMetricRecord.from_dict(obj)
        else:
            return SchemaMetricPrimitive.from_dict(obj)
        raise ValueError(f"Unknown type: {_type}")

    @classmethod
    def from_ldms_metric_template(cls, mt:ldms.MetricTemplate):
        if mt.type == ldms.LDMS_V_RECORD_TYPE:
            return SchemaMetricRecord.from_ldms_metric_template(mt)
        elif mt.type == ldms.LDMS_V_RECORD_ARRAY:
            return SchemaMetricRecordArray.from_ldms_metric_template(mt)
        elif mt.type == ldms.LDMS_V_LIST:
            return SchemaMetricList.from_ldms_metric_template(mt)
        elif ldms.type_is_array(mt.type):
            return SchemaMetricArray.from_ldms_metric_template(mt)
        return SchemaMetricPrimitive.from_ldms_metric_template(mt)

    @classmethod
    def from_json(cls, obj):
        if type(obj) is str:
            obj = json.loads(obj)
        return cls.from_dict(obj)

    def as_json(self):
        return json.dumps(self.as_dict())

    def as_dict(self):
        # Subclass shall override this
        raise NotImplementedError()

    def as_ldms_metric_desc(self):
        # Subclass shall override this
        raise NotImplementedError()

    def update_digest(self, h):
        h.update(self.name.encode())
        h.update(self.type.value.to_bytes(4, 'little'))

    def compatible(self, other):
        raise NotImplementedError()


class SchemaMetricPrimitive(SchemaMetric):
    def __init__(self, name, _type, doc = None, is_meta = False, units = None):
        super().__init__(name, _type, doc = doc, is_meta = is_meta, units = units)

    @classmethod
    def from_dict(cls, obj):
        name = obj["name"]
        _type = obj["type"]
        doc = obj.get("doc")
        is_meta = obj.get("is_meta")
        units = obj.get("units")
        return cls(name, _type, doc = doc, is_meta = is_meta, units = units)

    @classmethod
    def from_ldms_metric_template(cls, mt:ldms.MetricTemplate):
        return cls(mt.name, mt.type, is_meta = mt.is_meta,
                   units = mt.units)

    def as_dict(self):
        return {
                "name": self.name,
                "type": self.type.to_json_str(),
                "is_meta" : self.is_meta,
                "units" : self.units,
                "doc": self.doc
            }

    def as_ldms_metric_desc(self):
        ret = { "name": self.name, "metric_type": self.type.to_json_str() }
        if self.is_meta:
            ret['meta'] = self.is_meta
        if self.units:
            ret['units'] = self.units
        return ret

    def compatible(self, other):
        return self.name == other.name and \
               self.type == other.type and \
               self.units == other.units and \
               self.is_meta == other.is_meta


class SchemaMetricArray(SchemaMetric):
    def __init__(self, name, item_type, _len, is_meta = False, units = None, doc = None):
        array_type = ARRAY_TBL.get(item_type)
        super().__init__(name, array_type, doc = doc, is_meta = is_meta, units = units)
        self.item_type = ITEM_TYPE_TBL.get(item_type)
        self.len = _len

    @classmethod
    def from_dict(cls, obj):
        name = obj['name']
        _type = obj['type']
        if _type != "array":
            raise ValueError("not an 'array' type")
        item_type = obj['items']
        array_type = ARRAY_TBL.get(item_type)
        if array_type == ValueType.RECORD_ARRAY:
            return SchemaMetricRecordArray.from_dict(obj)
        _len = obj.get('len', -1)
        is_meta = obj.get('is_meta')
        units = obj.get('units')
        doc = obj.get('doc')
        return cls(name, item_type, _len, is_meta = is_meta, units = units,
                   doc = doc)

    @classmethod
    def from_ldms_metric_template(cls, mt:ldms.MetricTemplate):
        return cls(mt.name, mt.type, mt.count, is_meta = mt.is_meta, units = mt.units)

    def as_dict(self):
        return {
                "name": self.name,
                "type": "array",
                "is_meta" : self.is_meta,
                "units" : self.units,
                "doc" : self.doc,
                "items": self.item_type.to_json_str(),
                "len": self.len,
            }

    def as_ldms_metric_desc(self):
        ret = { "name": self.name, "metric_type": self.type.to_json_str(),
                "count": self.len }
        if self.is_meta:
            ret['meta'] = self.is_meta
        if self.units:
            ret['units'] = self.units
        return ret

    def compatible(self, other):
        return self.name == other.name and \
               self.type == other.type and \
               self.item_type == other.item_type and \
               self.len == other.len and \
               self.units == other.units and \
               self.is_meta == other.is_meta

class SchemaMetricList(SchemaMetric):
    def __init__(self, name, heap_sz, is_meta = False, units = None, doc = None):
        super().__init__(name, ValueType.LIST, is_meta = is_meta, units = units, doc = doc)
        self.heap_sz = heap_sz

    @classmethod
    def from_dict(cls, obj):
        name = obj['name']
        _type = obj['type']
        if _type != "list":
            raise ValueError("not a 'list' type")
        heap_sz = obj.get('heap_sz', -1)
        return cls(name, heap_sz)

    @classmethod
    def from_ldms_metric_template(cls, mt:ldms.MetricTemplate):
        return cls(mt.name, mt.count, units = mt.units)

    def as_dict(self):
        return {
                "name": self.name,
                "type": "list",
                "is_meta" : self.is_meta,
                "units" : self.units,
                "doc" : self.doc,
                "heap_sz": self.heap_sz,
            }

    def as_ldms_metric_desc(self):
        ret = { "name": self.name, "metric_type": self.type.to_json_str(),
                "count": self.heap_sz }
        if self.is_meta:
            ret['meta'] = self.is_meta
        if self.units:
            ret['units'] = self.units
        return ret

    def compatible(self, other):
        return self.name == other.name and \
               self.type == other.type and \
               self.units == other.units and \
               self.is_meta == other.is_meta


class SchemaMetricRecord(SchemaMetric):
    def __init__(self, name, metrics, is_meta = True, units = None, doc = None):
        super().__init__(name, ValueType.RECORD_TYPE,
                         is_meta = is_meta, units = units, doc = doc)
        self.metrics = metrics

    @classmethod
    def from_dict(self, json_obj):
        _type = json_obj["type"]
        if not _type:
            raise ValueError(f"Missing 'type' attribute")
        if _type != 'record':
            raise ValueError(f"Expecting 'record' type but got '{_type}'")
        name = json_obj.get("name")
        if not name:
            raise ValueError(f"Missing 'name' attribute")
        fields = json_obj.get("fields")
        if not fields:
            raise ValueError(f"Missing 'fields' attribute")
        metrics = list()
        name_set = set()
        for f in fields:
            # name, doc, type
            f_name = f.get('name')
            f_type = f.get('type')
            if not f_name:
                raise ValueError(f"Missing 'name' attribute in the record field")
            if not f_type:
                raise ValueError(f"Missing 'type' attribute in the record field")
            if f_name in name_set:
                raise ValueError(f"Field '{f_name}' appeared multiple times")
            name_set.add(f_name)
            m = SchemaMetric.from_json(f)
            metrics.append(m)
        return SchemaMetricRecord(name, metrics)

    @classmethod
    def from_ldms_metric_template(cls, mt:ldms.MetricTemplate):
        mlist = list()
        for m in mt.rec_def:
            _m = SchemaMetric.from_ldms_metric_template(m)
            mlist.append(_m)
        return cls(mt.name, mlist, is_meta = mt.is_meta, units = mt.units)

    def as_dict(self):
        return {
                "name": self.name,
                "type": "record",
                "is_meta" : bool(self.is_meta),
                "units" : self.units,
                "doc" : self.doc,
                "fields": [ m.as_dict() for m in self.metrics ],
                }

    def update_digest(self, h):
        # digest the members first
        for m in self.metrics:
            m.update_digest(h)
        h.update(self.name.encode())
        h.update(self.type.value.to_bytes(4, 'little'))

    def as_ldms_metric_desc(self):
        mlist = [ m.as_ldms_metric_desc() for m in self.metrics ]
        recdef = ldms.RecordDef(self.name, mlist)
        ret = { "name": self.name, "metric_type": recdef }
        if self.is_meta:
            ret['meta'] = self.is_meta
        if self.units:
            ret['units'] = self.units
        return ret

    def compatible(self, other):
        if type(other) != SchemaMetricRecord:
            return False
        if len(self.metrics) != len(other.metrics):
            return False
        for m0, m1 in zip(self.metrics, other.metrics):
            if not m0.compatible(m1):
                return False
        return self.name == other.name and \
               self.type == other.type and \
               self.units == other.units and \
               self.is_meta == other.is_meta


class SchemaMetricRecordArray(SchemaMetricArray):
    def __init__(self, name, record_type, _len, is_meta = False,
                 units = None, doc = None):
        super().__init__(name = name, item_type = "record",
                         _len = _len, is_meta = is_meta, units = units,
                         doc = doc)
        self.record_type = record_type

    @classmethod
    def from_dict(cls, obj):
        name = obj['name']
        _type = obj['type']
        if _type != "array":
            raise ValueError("not an 'array' type")
        item_type = obj['items']
        array_type = ARRAY_TBL.get(item_type)
        if array_type != ValueType.RECORD_ARRAY:
            raise ValueError("Not a record array")
        record_type = obj["record_type"]
        _len = obj.get('len', -1)
        is_meta = obj.get('is_meta')
        units = obj.get('units')
        doc = obj.get('doc')
        return cls(name = name, record_type = record_type, _len = _len,
                   is_meta = is_meta, units = units,
                   doc = doc)

    @classmethod
    def from_ldms_metric_template(cls, mt:ldms.MetricTemplate):
        assert(mt.type == ldms.LDMS_V_RECORD_ARRAY)
        record_type = mt.rec_def.name
        return cls(name = mt.name, record_type = record_type,
                   _len = mt.count, is_meta = mt.is_meta, units = mt.units)

    def as_dict(self):
        return {
                "name": self.name,
                "type": "array",
                "is_meta" : self.is_meta,
                "units" : self.units,
                "doc" : self.doc,
                "items": "record",
                "record_type": self.record_type,
                "len": self.len,
            }

    def as_ldms_metric_desc(self, rec_def):
        ret = { "name": self.name, "metric_type": "record[]", "count": self.len }
        if self.is_meta:
            ret['meta'] = self.is_meta
        if self.units:
            ret['units'] = self.units
        if type(rec_def) != ldms.RecordDef:
            raise TypeError(f"Type of `rec_def` must be RecordDef, got '{type(rec_def)}'")
        ret['rec_def'] = rec_def
        return ret


class Schema(object):
    def __init__(self, name:str, metrics:list, doc = None):
        self.name = name
        self.metrics = metrics
        self.doc = doc

    def as_dict(self):
        """Return the Schema, encoded in JSON"""
        return {
                "schema": {
                    "name": self.name,
                    "type": "record",
                    "doc" : self.doc,
                    "fields": [ m.as_dict() for m in self.metrics ],
                },
             }

    def as_ldms_schema(self):
        sch = ldms.Schema(self.name)
        _mdict = dict()
        for m in self.metrics:
            if m.type == ValueType.RECORD_ARRAY:
                _rt = _mdict.get(m.record_type)
                if _rt is None:
                    raise ValueError(f"RECORD_ARRAY uses unknown RECORD_TYPE:" \
                                     f" {m.record_type}")
                rec_def = _rt['metric_type']
                if type(rec_def) is not ldms.RecordDef:
                    raise TypeError(f"RECORD_ARRAY refers to a non-record type")
                _m = m.as_ldms_metric_desc(rec_def = rec_def)
            else:
                _m = m.as_ldms_metric_desc()
            _mdict[m.name] = _m
            sch.add_metric(**_m)
        return sch

    @classmethod
    def from_ldms_schema(cls, sch:ldms.Schema):
        mlist = list()
        for mt in sch:
            m = SchemaMetric.from_ldms_metric_template(mt)
            mlist.append(m)
        return cls(sch.name, mlist)

    @classmethod
    def from_ldms_set(cls, lset:ldms.Set):
        n = len(lset)
        mtlist = lset.get_metric_info(range(0, n))
        mlist = [ SchemaMetric.from_ldms_metric_template(mt) for mt in mtlist ]
        return cls(lset.schema_name, mlist)

    def as_json(self):
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_obj):
        if type(json_obj) == bytes:
            json_obj = json.loads(json_obj.decode())
        elif type(json_obj) == str:
            json_obj = json.loads(json_obj)
        return cls.from_dict(json_obj)

    @classmethod
    def from_dict(cls, json_obj):
        sch_def = json_obj.get("schema", json_obj)
        _type = sch_def["type"]
        # Expect only 'record' for now
        # * The Apache Avro schema definition could be of other types, e.g.
        #   array, or enum.
        # * LDMS set schema is represented as AVRO 'record'
        if _type != "record":
            raise ValueError(f"Unsupported schema type: {_type}")
        rec = SchemaMetricRecord.from_dict(sch_def)
        return Schema(rec.name, rec.metrics, rec.doc)

    @property
    def digest(self):
        h = hashlib.sha256()
        for m in self.metrics:
            m.update_digest(h)
        return h.digest()

    @property
    def id(self):
        return f"{self.name}-{self.digest.hex()}"

    def compatible(self, other):
        # Recursively compare everything, except 'heap_sz'
        if len(self.metrics) != len(other.metrics):
            return False
        for m0, m1 in zip(self.metrics, other.metrics):
            if not m0.compatible(m1):
                return False
        return True


class EtcdProxy(object):
    def __init__(self, etcd_list):
        """Initialization

        @param etcd_list(list): a list of `{'host': _HOST_, 'port': _PORT_}`
        """
        self.etcd = None
        self.lock = threading.Lock()
        self.etcd_idx = 0
        self.etcd_list = etcd_list
        self.__next_etcd_client()

        proxy_fn_list = [ "get", "get_prefix", "put", "put_if_not_exists",
                          "replace", "transaction", "delete", "delete_prefix" ]
        for p in proxy_fn_list:
            etcd_fn = getattr(etcd3.Etcd3Client, p)
            self.__proxy_fn_wrap(p, etcd_fn)

    def __proxy_fn_wrap(self, name, etcd_fn):
        @wraps(etcd_fn)
        def _proxy(*args, **kwargs):
            self.lock.acquire()
            n = len(self.etcd_list)
            for i in range(0, n):
                try:
                    val = etcd_fn(self.etcd, *args, **kwargs)
                except etcd3.Etcd3Exception as e:
                    # Try next client when we encounter Etcd3Exception
                    self.__next_etcd_client()
                except Exception as e:
                    # For other Exception, raise it
                    self.lock.release()
                    raise # re-raise the exception
                else:
                    break # success
            else:
                # All etcd failed
                self.lock.release()
                raise # re-raise the exception
            self.lock.release()
            return val
        setattr(self, name, _proxy)

    @property
    def transactions(self):
        return self.etcd.transactions

    def __next_etcd_client(self):
        # no lock
        if self.etcd:
            self.etcd.close()
        member = self.etcd_list[self.etcd_idx]
        self.etcd_idx += 1
        self.etcd_idx %= len(self.etcd_list)
        self.etcd = etcd3.client(host=member['host'], port=member['port'])
        return self.etcd


class SchemaRegistry(object):
    def __init__(self, etcd_spec):
        self.app = Flask(__name__)
        self.app.url_map.strict_slashes = False # 'link' and 'link/' both work
        self.app.sr = self # so that web app can use our resources
        self.app.register_blueprint(blueprint)
        etcd_list = etcd_spec.get('members', [{'host': 'localhost', 'port':'2379'}])
        self.lock = threading.Lock()
        sr_conf = etcd_spec.get('schema_registry', {'listen': '*.8080'} )
        self.listen = sr_conf.get('listen', "*:8080")
        self.srv_thr = None
        self.keyfile = sr_conf.get('keyfile')
        self.certfile = sr_conf.get('certfile')
        self.etcd = EtcdProxy(etcd_list)
        auth_conf = sr_conf.get('auth')
        self.auth = get_auth(auth_conf)
        self.etcd_prefix = sr_conf.get('etcd_prefix', '/schema-registry')
        self._DIGESTS_PREFIX = f"{self.etcd_prefix}/index/digests"
        self._NAMES_PREFIX = f"{self.etcd_prefix}/index/names"
        self._OBJECTS_PREFIX = f"{self.etcd_prefix}/objects"

    def start(self):
        self.lock.acquire()
        if self.srv_thr:
            self.lock.release()
            raise RuntimeError("HTTP server already running")
        self.srv_thr = threading.Thread(target = srv_proc,
                            kwargs = dict(app = self.app,
                                          listen = self.listen,
                                          keyfile = self.keyfile,
                                          certfile = self.certfile))
        self.srv_thr.start()
        self.lock.release()

    def join(self):
        self.srv_thr.join()

    def _digest_key(self, digest, _id):
        digest_key = f"{self._DIGESTS_PREFIX}/{digest}/{_id}"
        return digest_key

    def _name_key(self, name, _id):
        name_key = f"{self._NAMES_PREFIX}/{name}/{_id}"
        return name_key

    def _obj_key(self, _id):
        obj_key = f"{self._OBJECTS_PREFIX}/{_id}"
        return obj_key

    def add_schema(self, sch:Schema):
        js = sch.as_json()
        # store the object
        digest = sch.digest.hex()
        _id = sch.id
        obj_key = self._obj_key(_id)
        obj_json = sch.as_json()
        new_put = self.etcd.put_if_not_exists(obj_key, obj_json)
        if not new_put:
            return
        # index by name
        name_key = self._name_key(sch.name, _id)
        self.etcd.put(name_key, f"{_id}")
        # index by digest
        digest_key = self._digest_key(digest, _id)
        self.etcd.put(digest_key, f"{_id}")

    def get_schema(self, _id):
        key = f'{self._OBJECTS_PREFIX}/{_id}'
        v, m = self.etcd.get(key)
        if not v:
            raise ValueError(f"Schema not found, id: {_id}")
        return Schema.from_json(v.decode())

    def list_names(self, name = None):
        """Returns a `dict` of { '_SCHEMA_NAME_': [ _SCHEMA_IDS_ ] }"""
        _name = f"{name}/" if name else ""
        prefix = f"{self._NAMES_PREFIX}/{_name}"
        g = self.etcd.get_prefix(prefix)
        ret = dict()
        for v, k in g:
            prefix, name, _id = k.key.decode().rsplit('/', 2)
            l = ret.setdefault(name, list())
            l.append(v.decode())
        return ret

    def purge_database(self):
        """Purge all data in etcd under 'self.etcd_prefix'"""
        self.etcd.delete_prefix(self.etcd_prefix)

    def delete_schema(self, _id):
        name, digest = _id.rsplit('-', 1)
        obj_key = self._obj_key(_id)
        digest_key = self._digest_key(digest, _id)
        name_key = self._name_key(name, _id)
        for k in [obj_key, digest_key, name_key]:
            self.etcd.delete(k)

    def list_digests(self, digest = None):
        """Returns a `dict` of { '_SCHEMA_DIGEST_': [ _SCHEMA_IDS_ ] }"""
        _digest = f"{digest}/" if digest else ""
        prefix = f"{self._DIGESTS_PREFIX}/{_digest}"
        g = self.etcd.get_prefix(prefix)
        ret = dict()
        for v, k in g:
            prefix, digest, _id = k.key.decode().rsplit('/', 2)
            l = ret.setdefault(digest, list())
            l.append(v.decode())
        return ret


class SchemaRegistryClient(object):
    """SchemaRegistryClient(urls, auth = None, ca_cert = None)

    A schema registry client to interact with the schema registry servers (over
    HTTP/HTTPS).

    :param urls: a list of schema registry server URls.
    :param auth: an authentication object from `requests.auth` package.
    :param ca_cert: an optional CA certificate for SSL/TLS verification. This is
                    useful for self-signed certificate.
    """
    def __init__(self, urls, auth = None, ca_cert = None):
        """
        """
        self._urls = list(urls)
        self._uidx = 0
        self.auth = auth
        self.ca_cert = ca_cert

    def _req(self, path, method = requests.get, json_obj = None):
        n = len(self._urls)
        while n:
            prefix = self._urls[self._uidx]
            url = f'{prefix}{path}'
            kwargs = dict(url=url)
            if self.auth:
                kwargs['auth'] = self.auth
            if self.ca_cert:
                kwargs['verify'] = self.ca_cert
            if json_obj:
                kwargs['json'] = json_obj
            try:
                resp = method(**kwargs)
            except:
                # failed to send request to the server, retry with another URL
                n -= 1
                self._uidx = (self._uidx + 1) % len(self._urls)
                continue
            if not resp.ok:
                # request completed with a not-OK status
                raise RuntimeError(f'HTTP Response Status: {resp.status_code}')
            return resp
        # Reaching here means that we exhausted all URLs, just re-raise the
        # latest exception.
        raise

    def get_schema(self, _id):
        path   = URL_SCHEMAS_IDS.replace('<_id>', _id)
        resp = self._req(path)
        return Schema.from_json(resp.content)

    def del_schema(self, _id):
        """Delete schema"""
        if not _id:
            raise ValueError(f'`_id` parameter is required')
        path   = URL_SCHEMAS_IDS.replace('<_id>', _id)
        resp = self._req(path, method = requests.delete)
        return resp

    def add_schema(self, s:Schema):
        resp = self._req("/", method = requests.post, json_obj = s.as_dict())
        return resp

    def delete_schema(self, _id):
        return self.del_schema(_id)

    def list_names(self):
        resp = self._req(URL_NAMES_DIR)
        objs = json.loads(resp.content.decode())
        return objs

    def list_versions(self, name=None, digest=None):
        """List schema versions (IDs) by `name` or `digest`"""
        if name:
            if digest:
                raise ValueError('Only `name` or `digest` paramter can be specified')
            path = URL_NAMES_VERSIONS_DIR.replace('<name>', name)
        elif digest:
            path = URL_DIGESTS_VERSIONS_DIR.replace('<digest>', digest)
        else:
            raise ValueError('`name` or `digest` parameter is required')
        resp = self._req(path)
        objs = json.loads(resp.content.decode())
        return objs

    def list_digests(self):
        resp = self._req(URL_DIGESTS_DIR)
        objs = json.loads(resp.content.decode())
        return objs


if __name__ == '__main__':
    # For running stand-alone schema_registry
    import argparse
    import yaml
    logging.basicConfig()
    parser = argparse.ArgumentParser(description="LDMS Schema Registry")
    parser.add_argument('-c', '--cluster', required=True, metavar='ETCD_YAML',
                        type=argparse.FileType('r'))
    args = parser.parse_args()
    etcd_spec = yaml.safe_load(args.cluster)

    # schemaregistry
    sr_config = etcd_spec.get('schema_registry')
    if sr_config:
        sr = SchemaRegistry(etcd_spec)
        sr.start()
        if False:
            sr.purge_database()
            o = open("/db/schema.json")
            obj = json.load(o)
            s = Schema.from_dict(obj)
            print(f"digest: {s.digest.hex()}")
            sr.add_schema(s)
        sys.flags.interactive or sr.join()
    else:
        raise RuntimeError('schema_registry config section not found')
