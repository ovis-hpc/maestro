#ifndef __LDMS_SCHEMA_ACCESS_H__
#define __LDMS_SCHEMA_ACCESS_H__

#include "ldms/ldms.h"

typedef struct ldms_mdef_s *ldms_mdef_t;

ldms_mdef_t ldms_schema_mdef_first(ldms_schema_t sch);
ldms_mdef_t ldms_mdef_next(ldms_mdef_t mdef);
enum ldms_value_type ldms_mdef_type(ldms_mdef_t mdef);
const char *ldms_mdef_name(ldms_mdef_t mdef);
const char *ldms_mdef_units(ldms_mdef_t mdef);
int ldms_mdef_array_len(ldms_mdef_t mdef);
int ldms_mdef_list_heap_sz(ldms_mdef_t mdef);
ldms_record_t ldms_mdef_record(ldms_mdef_t mdef);
ldms_mdef_t ldms_record_mdef_first(ldms_record_t rec);

int ldms_mdef_is_meta(ldms_mdef_t mdef);

#endif
