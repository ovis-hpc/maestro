#include <stdint.h>
#include <malloc.h>
#include <string.h>
#include <errno.h>
#include <curl/curl.h>
#include <jansson.h>

#include "msr_client.h"

/* #include "ldms_schema_access.h" */

#define URL_IDS "%s/schemas/ids/%s"
#define URL_NAMES_LIST "%s/names"
#define URL_NAMES_VERSIONS "%s/names/%s/versions"
#define URL_DIGESTS_LIST "%s/digests"
#define URL_DIGESTS_VERSIONS "%s/digests/%s/versions"

typedef struct msr_buf_s *msr_buf_t;
struct msr_buf_s {
	size_t off;  /* next write location */
	size_t alen; /* available length */
	char *buf;  /* the buffer */
};

typedef ldms_metric_template_t ldms_mdef_t;

struct msr_client_s {
	int n;
	int idx;
	const char *ca_cert; /* path to ca_cer */
	const char *urls[];

	/*
	 * msr_client_s contiguous memory format:
	 * - n
	 * - idx
	 * - ca_cert ------------.
	 * - urls[0] ------.     |
	 * - urls[1] ------+-.   |
	 * - ...           | |   |
	 * - urls[n-1] ----+-+-. |
	 * - urls_0_data <-' | | |
	 * - urls_1_data <---' | |
	 * - ...               | |
	 * - urls_(n-1)_data <-' |
	 * - ca_cert_data <------'
	 */
};

#define MSR_CHUNKSZ 0x2000
#define MSR_ROUNDUP(Z) ( ((Z)-1)&0x1FFF + 1 )

static msr_buf_t __msr_buf_new()
{
	msr_buf_t b = malloc(sizeof(*b));
	if (b) {
		b->off = 0;
		b->alen = MSR_CHUNKSZ;
		b->buf = malloc(MSR_CHUNKSZ);
		if (!b->buf) {
			free(b);
			b = NULL;
		}
	}
	return b;
}

static int __msr_buf_extend(msr_buf_t b, size_t sz_more)
{
	void *tmp;
	size_t sz = MSR_ROUNDUP(sz_more);
	tmp = realloc(b->buf, b->alen + b->off + sz);
	if (tmp) {
		b->buf = tmp;
		b->alen += sz;
		return 0;
	}
	return ENOMEM;
}

static void __msr_buf_free(msr_buf_t b)
{
	free(b->buf);
	free(b);
}

static size_t __curl_write_cb(void *data, size_t sz, size_t n, void *ctxt)
{
	size_t data_sz = sz * n;
	msr_buf_t b = ctxt;
	size_t sz_more;
	int rc;
	if (data_sz >= b->alen) {
		sz_more = data_sz - b->alen;
		rc = __msr_buf_extend(b, sz_more);
		if (rc)
			return 0;
	}
	memcpy(&b->buf[b->off], data, data_sz);
	b->off += data_sz;
	b->alen -= data_sz;
	b->buf[b->off] = 0; /* always '\0' terminated */
	return data_sz;
}

msr_client_t msr_client_new(const char *urls[], const char *ca_cert)
{
	int n, len;
	const char *u;
	msr_client_t cli;
	size_t sz;

	/* calculate size */
	sz = sizeof(*cli);
	for (n = 0; (u = urls[n]); n++) {
		len = strlen(u) + 1;
		sz += len;
	}
	if (ca_cert) {
		len = strlen(ca_cert) + 1;
		sz += len;
	}
	sz += sizeof(char*)*n;
	cli = malloc(sz);
	if (!cli)
		return NULL;
	cli->n = n;
	cli->idx = 0;
	u = (char*)&cli->urls[cli->n];
	for (n = 0; n < cli->n; n++) {
		cli->urls[n] = u;
		strcpy((char*)u, urls[n]);
		len = strlen(u) + 1;
		u += len;
	}
	if (ca_cert) {
		cli->ca_cert = u;
		strcpy((char*)u, ca_cert);
		len = strlen(u) + 1;
		u += len;
	} else {
		cli->ca_cert = NULL;
	}
	return cli;
}

struct __json_type_ent_s {
	const char *name;
	enum ldms_value_type type;
};

static struct __json_type_ent_s __tbl[] = {
	{ "d64",    LDMS_V_D64         },
	{ "double", LDMS_V_D64         },
	{ "f32",    LDMS_V_F32         },
	{ "float",  LDMS_V_F32         },
	{ "list",   LDMS_V_LIST        },
	{ "long",   LDMS_V_S64         },
	{ "record", LDMS_V_RECORD_TYPE },
	{ "s16",    LDMS_V_S16         },
	{ "s32",    LDMS_V_S32         },
	{ "s64",    LDMS_V_S64         },
	{ "s8",     LDMS_V_S8          },
	{ "u16",    LDMS_V_U16         },
	{ "u32",    LDMS_V_U32         },
	{ "u64",    LDMS_V_U64         },
	{ "u8",     LDMS_V_U8          },
};

static int __tbl_cmp(const void *_key, const void *_ent)
{
	const char *key = _key;
	const struct __json_type_ent_s *ent = _ent;
	return strcmp(key, ent->name);
}

enum ldms_value_type __json_field_type(json_t *o, int *_len)
{
	json_t *jtype = json_object_get(o, "type");
	json_t *jitems = NULL, *jlen = NULL, *jheap_sz = NULL;
	const char *stype = json_string_value(jtype);
	struct __json_type_ent_s *ent;
	enum ldms_value_type typ;

	*_len = 1;

	if (0 == strcmp(stype, "array")) {
		jitems = json_object_get(o, "items");
		stype = json_string_value(jitems); /* type of array element */
		jlen = json_object_get(o, "len");
		*_len = json_integer_value(jlen);
	}

	ent = bsearch(stype, __tbl, sizeof(__tbl)/sizeof(__tbl[0]),
				    sizeof(__tbl[0]), __tbl_cmp);
	if (!ent)
		return LDMS_V_NONE;
	typ = ent->type;
	if (jitems) {
		/* make it an array */;
		if (typ == LDMS_V_RECORD_TYPE) {
			typ = LDMS_V_RECORD_ARRAY;
		} else {
			typ += ( LDMS_V_CHAR_ARRAY - LDMS_V_CHAR );
		}
	}

	if (typ == LDMS_V_LIST) {
		jheap_sz = json_object_get(o, "heap_sz");
		*_len = json_integer_value(jheap_sz);
	}

	return typ;
}

static int __ldms_record_add_json_metric(ldms_record_t rec, json_t *obj)
{
	json_t *jname;
	int len, rc, idx;
	enum ldms_value_type lvt;
	if (json_typeof(obj) != JSON_OBJECT)
		goto einval;

	jname = json_object_get(obj, "name");
	if (!jname || json_typeof(jname) != JSON_STRING)
		goto einval;

	lvt = __json_field_type(obj, &len);

	switch (lvt) {
	case LDMS_V_S8:  case LDMS_V_U8:
	case LDMS_V_S16: case LDMS_V_U16:
	case LDMS_V_S32: case LDMS_V_U32:
	case LDMS_V_S64: case LDMS_V_U64:
	case LDMS_V_F32: case LDMS_V_D64:

	case LDMS_V_S8_ARRAY:  case LDMS_V_U8_ARRAY:
	case LDMS_V_S16_ARRAY: case LDMS_V_U16_ARRAY:
	case LDMS_V_S32_ARRAY: case LDMS_V_U32_ARRAY:
	case LDMS_V_S64_ARRAY: case LDMS_V_U64_ARRAY:
	case LDMS_V_F32_ARRAY: case LDMS_V_D64_ARRAY:
		idx = ldms_record_metric_add(rec, json_string_value(jname), NULL,
					    lvt, len);
		if (idx < 0) {
			rc = -idx;
			goto out;
		}
		break;
	case LDMS_V_NONE:
	default:
		goto einval;
	}
	rc = 0;
	goto out;

 einval:
	rc = EINVAL;
 out:
	return rc;
}

static int __ldms_schema_add_json_metric(ldms_schema_t sch, json_t *obj)
{
	json_t *jname, *jfields, *jv, *junits, *jmeta, *jrecord_type;
	int len, rc, idx, n, i;
	enum ldms_value_type lvt;
	ldms_record_t rec;
	const char *record_type;
	struct ldms_metric_template_s *tmp;
	if (json_typeof(obj) != JSON_OBJECT)
		goto einval;
	jname = json_object_get(obj, "name");
	if (!jname || json_typeof(jname) != JSON_STRING)
		goto einval;
	junits = json_object_get(obj, "units");
	jmeta = json_object_get(obj, "is_meta");
	lvt = __json_field_type(obj, &len);
	switch (lvt) {
	case LDMS_V_S8:  case LDMS_V_U8:
	case LDMS_V_S16: case LDMS_V_U16:
	case LDMS_V_S32: case LDMS_V_U32:
	case LDMS_V_S64: case LDMS_V_U64:
	case LDMS_V_F32: case LDMS_V_D64:
		/* primitive metric */
		if (json_is_true(jmeta)) {
			idx = ldms_schema_meta_add_with_unit(sch,
						json_string_value(jname),
						json_string_value(junits), lvt);
		} else {
			idx = ldms_schema_metric_add_with_unit(sch,
						json_string_value(jname),
						json_string_value(junits), lvt);
		}
		if (idx < 0) {
			rc = -idx;
			goto out;
		}
		break;
	case LDMS_V_S8_ARRAY:  case LDMS_V_U8_ARRAY:
	case LDMS_V_S16_ARRAY: case LDMS_V_U16_ARRAY:
	case LDMS_V_S32_ARRAY: case LDMS_V_U32_ARRAY:
	case LDMS_V_S64_ARRAY: case LDMS_V_U64_ARRAY:
	case LDMS_V_F32_ARRAY: case LDMS_V_D64_ARRAY:
		/* array of primitives */
		if (json_is_true(jmeta)) {
			idx = ldms_schema_meta_array_add_with_unit(sch,
					json_string_value(jname),
					json_string_value(junits),
					lvt, len);
		} else {
			idx = ldms_schema_metric_array_add(sch,
					json_string_value(jname), lvt, len);
		}
		if (idx < 0) {
			rc = -idx;
			goto out;
		}
		break;
	case LDMS_V_RECORD_TYPE:
		/* "record" maps to RECORD_INST; but it actually defining the
		 * type in JSON. */
		jfields = json_object_get(obj, "fields");
		if (!jfields || json_typeof(jfields) != JSON_ARRAY)
			goto einval;
		rec = ldms_record_create(json_string_value(jname));
		if (!rec) {
			rc = errno;
			goto out;
		}
		json_array_foreach(jfields, idx, jv) {
			if (json_typeof(jv) != JSON_OBJECT)
				goto einval;
			rc = __ldms_record_add_json_metric(rec, jv);
			if (rc)
				goto out;
		}
		idx = ldms_schema_record_add(sch, rec);
		if (idx < 0) {
			rc = -idx;
			goto out;
		}
		rec = NULL; /* rec now belongs to the schema */
		break;
	case LDMS_V_RECORD_ARRAY:
		/* get the rec_def first */
		jrecord_type = json_object_get(obj, "record_type");
		if (!json_is_string(jrecord_type))
			goto einval;
		record_type = json_string_value(jrecord_type);
		n = ldms_schema_metric_count_get(sch);
		tmp = malloc(sizeof(*tmp)*n);
		if (!tmp) {
			rc = errno;
			goto out;
		}
		ldms_schema_bulk_template_get(sch, n, tmp);
		for (i = 0; i < n; i++) {
			if (0 == strcmp(tmp[i].name, record_type))
				break;
		}
		if (i == n) {
			/* not found */
			free(tmp);
			rc = EINVAL;
			goto out;
		}
		idx = ldms_schema_record_array_add(sch, json_string_value(jname),
					     tmp[i].rec_def, len);
		free(tmp);
		if (idx < 0) {
			rc = -idx;
			goto out;
		}
		break;
	case LDMS_V_LIST:
		idx = ldms_schema_metric_list_add(sch, json_string_value(jname),
						 NULL, len);
		if (idx < 0) {
			rc = -idx;
			goto out;
		}
		break;
	case LDMS_V_NONE:
	default:
		goto einval;
	}
	rc = 0;
	goto out;

 einval:
	rc = EINVAL;
 out:
	return rc;
}

static ldms_schema_t __json_to_ldms_schema(json_t *obj)
{
	json_t *_obj = NULL, *v, *a;
	size_t idx;
	int rc;
	ldms_schema_t sch = NULL;
	if (json_typeof(obj) != JSON_OBJECT) {
		errno = EINVAL;
		goto err;
	}

	_obj = json_object_get(obj, "schema");
	if (_obj) { /* borrowed ref, do not decref(_obj) */
		obj = _obj;
	}

	v = json_object_get(obj, "name"); /* borrowed ref, do not decref(v) */
	if (!v)
		goto einval;
	if (json_typeof(v) != JSON_STRING)
		goto einval;
	sch = ldms_schema_new(json_string_value(v));
	v = NULL;
	a = json_object_get(obj, "fields");
	if (!a)
		goto einval;
	json_array_foreach(a, idx, v) {
		rc = __ldms_schema_add_json_metric(sch, v);
		if (rc) {
			errno = rc;
			goto err;
		}
	}
	return sch;

 einval:
	errno = EINVAL;
 err:
	if (sch)
		ldms_schema_delete(sch);
	return NULL;
}

static msr_buf_t __msr_url_post(msr_client_t msr, const char *url, json_t *obj,
				CURLcode *_res)
{
	msr_buf_t b = NULL;
	CURL *curl = NULL;
	char *obj_str = NULL;
	struct curl_slist *hdrs = NULL;
	CURLcode res;
	b = __msr_buf_new();
	if (!b)
		goto out;
	curl = curl_easy_init();
	if (!curl)
		goto err;

	res = curl_easy_setopt(curl, CURLOPT_URL, url);
	if (res != CURLE_OK)
		goto err;
	if (msr->ca_cert) {
		res = curl_easy_setopt(curl, CURLOPT_CAINFO, msr->ca_cert);
		if (res != CURLE_OK)
			goto err;
	}

	/* POST method */
	res = curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
	if (res != CURLE_OK)
		goto err;

	/* http request header & POST method */
	hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
	res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
	if (res != CURLE_OK)
		goto err;

	/* POST data (json) */
	obj_str = json_dumps(obj, 0);
	if (!obj_str)
		goto err;
	res = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, obj_str);
	if (res != CURLE_OK)
		goto err;

	/* setup recv data buffer */
	res = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, __curl_write_cb);
	if (res != CURLE_OK)
		goto err;
	res = curl_easy_setopt(curl, CURLOPT_WRITEDATA, b);
	if (res != CURLE_OK)
		goto err;

	res = curl_easy_perform(curl);
	if (res != CURLE_OK)
		goto err;

	goto out;

 err:
	if (b) {
		__msr_buf_free(b);
		b = NULL;
	}
 out:
	if (hdrs)
		curl_slist_free_all(hdrs);
	if (_res)
		*_res = res;
	if (curl)
		curl_easy_cleanup(curl);
	if (obj_str)
		free(obj_str);
	return b;
}

json_t *__ldms_mdef_to_json(ldms_mdef_t mdef);

json_t *__ldms_record_to_json(const char *name, ldms_record_t rec)
{
	json_t *obj = NULL;
	json_t *fields = NULL, *jval;
	int rc, i, n;
	struct ldms_metric_template_s *mdef = NULL;
	obj = json_object();
	rc = json_object_set_new(obj, "name", json_string(name));
	if (rc)
		goto err;
	rc = json_object_set_new(obj, "type", json_string("record"));
	if (rc)
		goto err;
	fields = json_array();
	if (!fields)
		goto err;
	rc = json_object_set_new(obj, "fields", fields);
	if (rc) {
		json_decref(fields);
		goto err;
	}
	n = ldms_record_metric_card(rec);
	mdef = malloc(n * sizeof(mdef[0]));
	ldms_record_bulk_template_get(rec, n, mdef);
	for (i = 0; i < n; i++) {
		jval = __ldms_mdef_to_json(&mdef[i]);
		if (!jval)
			goto err;
		rc = json_array_append(fields, jval);
		if (rc) {
			json_decref(jval);
			goto err;
		}
	}
	goto out;
 err:
	if (obj) {
		json_decref(obj);
		obj = NULL;
	}
 out:
	if (mdef)
		free(mdef);
	return obj;
}

json_t *__ldms_mdef_to_json(ldms_mdef_t mdef)
{
	ldms_record_t rec;
	enum ldms_value_type vt;
	json_t *obj = NULL;
	json_t *jname = NULL, *jtype = NULL, *jitems = NULL, *jlen = NULL;
	json_t *jrecord_type = NULL;
	json_t *jheap_sz = NULL;
	char buf[128];
	const char *atype;
	const char *units;
	int alen, mlen;
	obj = json_object();
	if (!obj)
		goto err;
	jname = json_string(mdef->name);
	if (!jname)
		goto err;
	json_object_set_new(obj, "name", jname); /* obj owns jname */
	vt = mdef->type;
	switch (vt) {
	case LDMS_V_CHAR:
	case LDMS_V_U8:
	case LDMS_V_S8:
	case LDMS_V_U16:
	case LDMS_V_S16:
	case LDMS_V_U32:
	case LDMS_V_S32:
	case LDMS_V_U64:
	case LDMS_V_S64:
	case LDMS_V_F32:
	case LDMS_V_D64:
		jtype = json_string(ldms_metric_type_to_str(vt));
		if (!jtype)
			goto err;
		json_object_set_new(obj, "type", jtype);
		break;
	case LDMS_V_CHAR_ARRAY:
	case LDMS_V_U8_ARRAY:
	case LDMS_V_S8_ARRAY:
	case LDMS_V_U16_ARRAY:
	case LDMS_V_S16_ARRAY:
	case LDMS_V_U32_ARRAY:
	case LDMS_V_S32_ARRAY:
	case LDMS_V_U64_ARRAY:
	case LDMS_V_S64_ARRAY:
	case LDMS_V_F32_ARRAY:
	case LDMS_V_D64_ARRAY:
		/* type */
		jtype = json_string("array");
		if (!jtype)
			goto err;
		json_object_set_new(obj, "type", jtype);
		/* items */
		atype = ldms_metric_type_to_str(vt);
		alen = strlen(atype);
		snprintf(buf, sizeof(buf), "%.*s", alen-2, atype); /* less [] */
		jitems = json_string(buf);
		if (!jitems)
			goto err;
		json_object_set_new(obj, "items", jitems);
		/* len */
		mlen = mdef->len;
		jlen = json_integer(mlen);
		if (!jlen)
			goto err;
		json_object_set_new(obj, "len", jlen);
		break;
	case LDMS_V_LIST:
		/* type */
		jtype = json_string("list");
		if (!jtype)
			goto err;
		json_object_set_new(obj, "type", jtype);
		/* heap_sz */
		jheap_sz = json_integer(mdef->len);
		if (!jheap_sz)
			goto err;
		json_object_set_new(obj, "heap_sz", jheap_sz);
		break;
	case LDMS_V_RECORD_TYPE:
		/* type */
		json_decref(obj); /* discard the object */
		rec = mdef->rec_def;
		obj = __ldms_record_to_json(mdef->name, rec);
		break;
	case LDMS_V_RECORD_ARRAY:
		/* type */
		jtype = json_string("array");
		if (!jtype)
			goto err;
		json_object_set_new(obj, "type", jtype);
		/* items */
		atype = ldms_metric_type_to_str(vt);
		alen = strlen(atype);
		snprintf(buf, sizeof(buf), "%.*s", alen-2, atype); /* less [] */
		jitems = json_string(buf);
		if (!jitems)
			goto err;
		json_object_set_new(obj, "items", jitems);
		/* len */
		mlen = mdef->len;
		jlen = json_integer(mlen);
		if (!jlen)
			goto err;
		json_object_set_new(obj, "len", jlen);
		/* record_type */
		jrecord_type = json_string(ldms_record_name_get(mdef->rec_def));
		if (!jrecord_type)
			goto err;
		json_object_set_new(obj, "record_type", jrecord_type);
		break;
	case LDMS_V_LIST_ENTRY:
	case LDMS_V_RECORD_INST:
	case LDMS_V_TIMESTAMP:
	case LDMS_V_NONE:
	default:
		errno = EINVAL;
		goto err;
	}

	units = mdef->unit;
	if (units) {
		json_object_set_new(obj, "units", json_string(units));
	}

	if (mdef->flags & LDMS_MDESC_F_META) {
		json_object_set_new(obj, "is_meta", json_boolean(1));
	}

	goto out;

 err:
	if (obj) {
		json_decref(obj);
		obj = NULL;
	}
 out:
	return obj;
}

json_t *__ldms_schema_to_json(ldms_schema_t sch)
{
	json_t *obj, *fields, *f;
	int rc, i, n;
	const char *name;
	struct ldms_metric_template_s *mdef = NULL;

	obj = json_object();
	if (!obj)
		goto out;
	rc = json_object_set_new(obj, "type", json_string("record"));
	if (rc)
		goto err;
	name = ldms_schema_name_get(sch);
	rc = json_object_set_new(obj, "name", json_string(name));
	if (rc)
		goto err;
	fields = json_array();
	rc = json_object_set_new(obj, "fields", fields);
	/* NOTE: `fields` ref is stolen; `fields` belongs to `obj` */
	if (rc)
		goto err;
	n = ldms_schema_metric_count_get(sch);
	mdef = malloc(n*sizeof(mdef[0]));
	if (!mdef)
		goto err;
	ldms_schema_bulk_template_get(sch, 1024, mdef);
	for (i = 0; i < n; i++) {
		f = __ldms_mdef_to_json(&mdef[i]);
		if (!f)
			goto err;
		json_array_append_new(fields, f); /* f belongs to fields */
	}

	goto out;

 err:
	if (obj)
		json_decref(obj);
	obj = NULL;
 out:
	if (mdef)
		free(mdef);
	return obj;
}

int msr_ldms_schema_add(msr_client_t msr, ldms_schema_t sch, char **id_out)
{
	json_t *obj = NULL;
	json_t *jid = NULL;
	json_error_t jerr;
	int rc = ENOSYS;
	CURLcode res;
	msr_buf_t b = NULL;

	obj = __ldms_schema_to_json(sch);
	if (!obj) {
		rc = errno;
		goto out;
	}

	b = __msr_url_post(msr, msr->urls[msr->idx], obj, &res);
	if (!b) {
		rc = EINVAL;
		goto out;
	}
	json_decref(obj); /* drop old obj */

	obj = json_loads(b->buf, 0, &jerr);
	if (!json_is_object(obj)) {
		rc = EINVAL;
		goto out;
	}
	jid = json_object_get(obj, "id"); /* borrowed ref; decref not needed */
	if (!json_is_string(jid)) {
		rc = EINVAL;
		goto out;
	}
	*id_out = strdup(json_string_value(jid));
	rc = 0;

 out:
	if (obj)
		json_decref(obj);
	if (b)
		__msr_buf_free(b);
	return rc;
}

static msr_buf_t __msr_url_get(msr_client_t msr, const char *url, CURLcode *_res)
{
	msr_buf_t b = NULL;
	CURL *curl = NULL;
	CURLcode res;
	b = __msr_buf_new();
	if (!b)
		goto out;
	curl = curl_easy_init();
	if (!curl)
		goto err;

	res = curl_easy_setopt(curl, CURLOPT_URL, url);
	if (res != CURLE_OK)
		goto err;
	if (msr->ca_cert) {
		res = curl_easy_setopt(curl, CURLOPT_CAINFO, msr->ca_cert);
		if (res != CURLE_OK)
			goto err;
	}

	/* setup data buffer */
	res = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, __curl_write_cb);
	if (res != CURLE_OK)
		goto err;
	res = curl_easy_setopt(curl, CURLOPT_WRITEDATA, b);
	if (res != CURLE_OK)
		goto err;

	res = curl_easy_perform(curl);
	if (res != CURLE_OK)
		goto err;

	goto out;
 err:
	if (b) {
		__msr_buf_free(b);
		b = NULL;
	}
 out:
	if (_res)
		*_res = res;
	if (curl)
		curl_easy_cleanup(curl);
	return b;
}

static msr_buf_t __msr_url_del(msr_client_t msr, const char *url, CURLcode *_res)
{
	msr_buf_t b = NULL;
	CURL *curl = NULL;
	CURLcode res;
	b = __msr_buf_new();
	if (!b)
		goto out;
	curl = curl_easy_init();
	if (!curl)
		goto err;

	res = curl_easy_setopt(curl, CURLOPT_URL, url);
	if (res != CURLE_OK)
		goto err;
	if (msr->ca_cert) {
		res = curl_easy_setopt(curl, CURLOPT_CAINFO, msr->ca_cert);
		if (res != CURLE_OK)
			goto err;
	}

	/* DELETE method */
	res = curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
	if (res != CURLE_OK)
		goto err;

	/* setup data buffer */
	res = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, __curl_write_cb);
	if (res != CURLE_OK)
		goto err;
	res = curl_easy_setopt(curl, CURLOPT_WRITEDATA, b);
	if (res != CURLE_OK)
		goto err;

	res = curl_easy_perform(curl);
	if (res != CURLE_OK)
		goto err;

	goto out;
 err:
	if (b) {
		__msr_buf_free(b);
		b = NULL;
	}
 out:
	if (_res)
		*_res = res;
	if (curl)
		curl_easy_cleanup(curl);
	return b;
}

ldms_schema_t msr_ldms_schema_get(msr_client_t msr, const char *id)
{
	int n;
	ldms_schema_t sch = NULL;
	CURLcode res;
	char url[BUFSIZ];
	msr_buf_t b = NULL;
	json_t *obj = NULL;
	json_error_t jerr;

	n = snprintf(url, sizeof(url), URL_IDS, msr->urls[msr->idx], id);
	if (n >= sizeof(url)) {
		/* unlikely */
		errno = ENAMETOOLONG;
		goto out;
	}

	b = __msr_url_get(msr, url, &res);
	if (!b)
		goto out;

	obj = json_loads(b->buf, 0, &jerr);
	if (!obj) {
		errno = EINVAL;
		goto out;
	}
	sch = __json_to_ldms_schema(obj);
 out:
	if (b)
		__msr_buf_free(b);
	if (obj)
		json_decref(obj);
	return sch;
}

int msr_ldms_schema_del(msr_client_t msr, const char *id)
{
	int rc;
	int n;
	CURLcode res;
	char url[BUFSIZ];
	msr_buf_t b = NULL;
	json_t *obj = NULL, *jid;
	json_error_t jerr;

	n = snprintf(url, sizeof(url), URL_IDS, msr->urls[msr->idx], id);
	if (n >= sizeof(url)) {
		/* unlikely */
		rc = ENAMETOOLONG;
		goto out;
	}

	b = __msr_url_del(msr, url, &res);
	if (!b) {
		rc = errno;
		goto out;
	}

	obj = json_loads(b->buf, 0, &jerr);
	if (json_typeof(obj) != JSON_ARRAY) {
		rc = EINVAL;
		goto out;
	}
	jid = json_array_get(obj, 0); /* borrowed ref; decref not needed */
	if (!jid || json_typeof(jid) != JSON_STRING) {
		rc = EINVAL;
		goto out;
	}
	if (strcmp(json_string_value(jid), id)) {
		rc = EINVAL;
		goto out;
	}
	rc = 0;
 out:
	if (b)
		__msr_buf_free(b);
	if (obj)
		json_decref(obj);
	return rc;
}

msr_results_t msr_names_list(msr_client_t msr)
{
	CURLcode res;
	int n;
	char url[BUFSIZ];
	msr_buf_t b = NULL;
	json_t *obj = NULL;
	json_t *v;
	json_error_t jerr;
	msr_results_t mres = NULL;
	size_t msz;
	char *s;

	n = snprintf(url, sizeof(url), URL_NAMES_LIST, msr->urls[msr->idx]);
	if (n >= sizeof(url)) {
		/* unlikely */
		errno = ENAMETOOLONG;
		goto out;
	}

	b = __msr_url_get(msr, url, &res);
	if (!b)
		goto out;
	obj = json_loads(b->buf, 0, &jerr);
	if (!obj) {
		errno = EINVAL;
		goto out;
	}
	if (json_typeof(obj) != JSON_ARRAY) {
		errno = EINVAL;
		goto out;
	}

	/* calculate size */
	msz = sizeof(*mres);
	json_array_foreach(obj, n, v) {
		msz += sizeof(char *);
		if (json_typeof(v) != JSON_STRING) {
			errno = EINVAL;
			goto out;
		}
		msz += strlen(json_string_value(v)) + 1;
	}
	mres = malloc(msz);
	if (!mres)
		goto out;

	/* now fill the values */
	mres->n = n;
	s = (char*)&mres->names[n];
	json_array_foreach(obj, n, v) {
		strcpy(s, json_string_value(v));
		mres->names[n] = s;
		s += strlen(s) + 1;
	}
	assert( ((void*)mres) + msz == (void*)s );
 out:
	if (b)
		__msr_buf_free(b);
	return mres;
}

static int __digest_from_str(ldms_digest_t d, const char *s)
{
	const char *p;
	int n;
	p = s;
	n = 0;
	while (*p && n < sizeof(d->digest)) {
		sscanf(p, "%02hhx", &d->digest[n]);
		n++;
		p+=2;
	}
	if (*p || n < sizeof(d->digest)) {
		return EINVAL;
	}
	return 0;
}

msr_results_t msr_digests_list(msr_client_t msr)
{
	CURLcode res;
	int n, rc;
	char url[BUFSIZ];
	msr_buf_t b = NULL;
	json_t *obj = NULL;
	json_t *v;
	json_error_t jerr;
	msr_results_t mres = NULL;
	size_t msz;

	n = snprintf(url, sizeof(url), URL_DIGESTS_LIST, msr->urls[msr->idx]);
	if (n >= sizeof(url)) {
		/* unlikely */
		errno = ENAMETOOLONG;
		goto out;
	}

	b = __msr_url_get(msr, url, &res);
	if (!b)
		goto out;
	obj = json_loads(b->buf, 0, &jerr);
	if (!obj) {
		errno = EINVAL;
		goto out;
	}
	if (json_typeof(obj) != JSON_ARRAY) {
		errno = EINVAL;
		goto out;
	}

	/* calculate size */
	n = json_array_size(obj);
	msz = sizeof(*mres) + n*sizeof(mres->digests[0]);
	mres = malloc(msz);
	if (!mres)
		goto out;

	/* now fill the values */
	mres->n = n;
	json_array_foreach(obj, n, v) {
		if (json_typeof(v) != JSON_STRING) {
			errno = EINVAL;
			goto err;
		}
		rc = __digest_from_str(&mres->digests[n], json_string_value(v));
		if (rc) {
			errno = rc;
			goto err;
		}
	}
	goto out;

 err:
	free(mres);
 out:
	if (b)
		__msr_buf_free(b);
	return mres;
}

msr_results_t msr_versions_list(msr_client_t msr, const char *name,
				ldms_digest_t digest)
{
	return msr_ids_list(msr, name, digest);
}

msr_results_t msr_ids_list(msr_client_t msr, const char *name,
			   ldms_digest_t digest)
{
	/* list ids by name or digest */
	CURLcode res;
	int n;
	char url[BUFSIZ];
	char digest_str[128];
	msr_buf_t b = NULL;
	json_t *obj = NULL;
	json_t *v;
	json_error_t jerr;
	msr_results_t mres = NULL;
	size_t msz;
	char *s;

	if (name) {
		n = snprintf(url, sizeof(url), URL_NAMES_VERSIONS,
					msr->urls[msr->idx], name);
		if (n >= sizeof(url)) {
			/* unlikely */
			errno = ENAMETOOLONG;
			goto out;
		}
	} else if (digest) {
		ldms_digest_str(digest, digest_str, sizeof(digest_str));
		n = snprintf(url, sizeof(url), URL_DIGESTS_VERSIONS,
					msr->urls[msr->idx], digest_str);
		if (n >= sizeof(url)) {
			/* unlikely */
			errno = ENAMETOOLONG;
			goto out;
		}
	} else {
		errno = EINVAL;
		goto out;
	}

	b = __msr_url_get(msr, url, &res);
	if (!b)
		goto out;
	obj = json_loads(b->buf, 0, &jerr);
	if (!obj) {
		errno = EINVAL;
		goto out;
	}
	if (json_typeof(obj) == JSON_NULL) {
		errno = ENOENT;
		goto out;
	}
	if (json_typeof(obj) != JSON_ARRAY) {
		errno = EINVAL;
		goto out;
	}

	/* calculate size */
	msz = sizeof(*mres);
	json_array_foreach(obj, n, v) {
		msz += sizeof(char *);
		if (json_typeof(v) != JSON_STRING) {
			errno = EINVAL;
			goto out;
		}
		msz += strlen(json_string_value(v)) + 1;
	}
	mres = malloc(msz);
	if (!mres)
		goto out;

	/* now fill the values */
	mres->n = n;
	s = (char*)&mres->ids[n];
	json_array_foreach(obj, n, v) {
		strcpy(s, json_string_value(v));
		mres->ids[n] = s;
		s += strlen(s) + 1;
	}
	assert( ((void*)mres) + msz == (void*)s );
 out:
	if (b)
		__msr_buf_free(b);
	return mres;
}

__attribute((constructor))
void __init__()
{
	curl_global_init(CURL_GLOBAL_DEFAULT);
}
