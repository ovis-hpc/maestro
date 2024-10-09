#include <openssl/evp.h>

#include "ldms_schema_access.h"
#include "ldms/ldms.h"

struct ldms_mdef_s {
	char *name;
	char *unit;
	enum ldms_value_type type;
	uint32_t flags;	/* DATA/MDATA flag */
	uint32_t count; /* Number of elements in the array if this is of an
			 * array type, or number of members if this is a
			 * record type. */
	size_t meta_sz;
	size_t data_sz;
	STAILQ_ENTRY(ldms_mdef_s) entry;
};

STAILQ_HEAD(metric_list_head, ldms_mdef_s);

struct ldms_schema_s {
	char *name;
	struct ldms_digest_s digest;
	EVP_MD_CTX *evp_ctx;
	int card;
	size_t meta_sz;
	size_t data_sz;
	int array_card;
	STAILQ_HEAD(, ldms_mdef_s) metric_list;
	LIST_ENTRY(ldms_schema_s) entry;
};

/*
 * This structure stores user-defined record definition constructed by
 * ldms_record_create() and ldms_record_metric_add() APIs. This structure will
 * later be used to create `ldms_record_type` in the set meta data.
 */
typedef struct ldms_record {
	struct ldms_mdef_s mdef; /* base */
	ldms_schema_t schema;
	int metric_id;
	int n; /* the number of members */
	size_t inst_sz; /* the size of an instance */
	size_t type_sz; /* the size of the record type (in metadata section) */
	STAILQ_HEAD(, ldms_mdef_s) rec_metric_list;
} *ldms_record_t;

typedef struct ldms_record_array_def {
	struct ldms_mdef_s mdef; /* base */
	int rec_type; /* index to the record type */
	int inst_sz;
} *ldms_record_array_def_t;

ldms_mdef_t ldms_schema_mdef_first(ldms_schema_t sch)
{
	return STAILQ_FIRST(&sch->metric_list);
}

ldms_mdef_t ldms_mdef_next(ldms_mdef_t mdef)
{
	return STAILQ_NEXT(mdef, entry);
}

enum ldms_value_type ldms_mdef_type(ldms_mdef_t mdef)
{
	return mdef->type;
}

const char *ldms_mdef_name(ldms_mdef_t mdef)
{
	return mdef->name;
}

const char *ldms_mdef_units(ldms_mdef_t mdef)
{
	return mdef->unit;
}

int ldms_mdef_array_len(ldms_mdef_t mdef)
{
	return mdef->count;
}

int ldms_mdef_list_heap_sz(ldms_mdef_t mdef)
{
	return mdef->count;
}

ldms_record_t ldms_mdef_record(ldms_mdef_t mdef)
{
	ldms_record_t rec = container_of(mdef, struct ldms_record, mdef);
	return rec;
}

ldms_mdef_t ldms_record_mdef_first(ldms_record_t rec)
{
	return STAILQ_FIRST(&rec->rec_metric_list);
}

int ldms_mdef_is_meta(ldms_mdef_t mdef)
{
	return !!(mdef->flags & LDMS_MDESC_F_META);
}
