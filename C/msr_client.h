/*
 */
#ifndef __MSR_CLIENT_H__
#define __MSR_CLIENT_H__

#include <ldms/ldms.h>

typedef struct msr_client_s *msr_client_t;

/**
 * \brief Create a Maestro Schema Registry (MSR) client.
 *
 * \param urls NULL terminated string array. Each element is the URL to the
 *             server.
 * \retval cli The client handler.
 */
msr_client_t msr_client_new(const char *urls[], const char *ca_cert);

typedef struct msr_results_s {
	int n;
	union {
		const char *names[0];
		const char *ids[0];
		struct ldms_digest_s digests[0];
	};
} *msr_results_t;

/**
 * Add a schema to MSR server.
 *
 * \retval 0 If success, or
 * \retval errno If error.
 */
int msr_ldms_schema_add(msr_client_t msr, ldms_schema_t schema, char **id_out);

ldms_schema_t msr_ldms_schema_get(msr_client_t msr, const char *id);
int msr_ldms_schema_del(msr_client_t msr, const char *id);

msr_results_t msr_names_list(msr_client_t msr);
msr_results_t msr_digests_list(msr_client_t msr);

msr_results_t msr_versions_list(msr_client_t msr, const char *name,
				ldms_digest_t digest);

msr_results_t msr_ids_list(msr_client_t msr, const char *name,
			   ldms_digest_t digest);


#endif
