#include <stdio.h>
#include <getopt.h>
#include <getopt.h>
#include <time.h>
#include <unistd.h>

#include <ldms/ldms.h>

#include "msr_client.h"

const char *short_opts = "aNDs:x:U:C:d:";

struct option long_opts[] = {
	{"add-schema",   0, 0, 'a'}, /* add a pre-defined test schema */
	{"del-schema",   1, 0, 'd'}, /* delete a schema */
	{"list-names",   0, 0, 'N'},
	{"list-digests", 0, 0, 'D'},
	{"xprt",         1, 0, 'x'},
	{"set-schema",   1, 0, 's'},
	{"urls",         1, 0, 'U'},
	{"ca-cert",      1, 0, 'C'},
	{0},
};

const char *__usage = "\
msr_test [-a|-N|-D| -x XPRT[:PORT[:ADDR]] -s SCHEMA_ID] \n\
	 -U SCHEMA_REGISTRY_LIST\n\
	 [-C CA_CERT_PATH]\n\
\n\
	 -a, -d, -N, -D, and -x are mutually exclusive operations.\n\
\n\
	 -a  to add a pre-defined schema to the schema registry.\n\
	 -N  to list schema ids by names.\n\
	 -D  to list schema ids by digests.\n\
\n\
	 -x XPRT[:PORT[:ADDR]]  -s SCHEMA_ID\n\
	    To listen to the given transport/port/addr and create an LDMS \n\
	    set with the schema from the schema registry with the specified \n\
	    SCHEMA_ID.\n\
\n\
	 -U SCHEMA_REGISTRY_URL_LIST\n\
	    A comma-separated list of schema registry URLs.\n\
\n\
	 [-C CA_CERT_PATH]\n\
	    An optional path to the custom CA Certificate (e.g. self-signed).\n\
";

void print_usage()
{
	printf("%s", __usage);
}

void do_add_schema(msr_client_t msr)
{
	int rc = 0, m;
	ldms_record_t rec;
	ldms_schema_t sch;
	char *id_test;

	struct ldms_metric_template_s rec_tmp[] = {
		{ "uno", LDMS_MDESC_F_DATA, LDMS_V_S64, "u_uno", },
		{ "dos", LDMS_MDESC_F_DATA, LDMS_V_S64, "u_dos", },
		{0}
	};
	rec = ldms_record_from_template("rec", rec_tmp, NULL);

	struct ldms_metric_template_s sch_tmp[] = {
		{ "one", LDMS_MDESC_F_DATA, LDMS_V_S64, "u_one", },
		{ "two", LDMS_MDESC_F_META, LDMS_V_S64, "u_two", },
		{ "three", LDMS_MDESC_F_DATA, LDMS_V_D64, "u_three", 10, },
		{ "rec", 0 /* ignored */, LDMS_V_RECORD_TYPE, NULL, 1, rec },
		{ "rec_array", LDMS_MDESC_F_DATA, LDMS_V_RECORD_ARRAY, NULL, 8, rec },
		{ "u32_array", LDMS_MDESC_F_DATA, LDMS_V_U32_ARRAY, NULL, 4, NULL },
		{ "list", LDMS_MDESC_F_DATA, LDMS_V_LIST, NULL, 512, },
		{0}
	};

	sch = ldms_schema_from_template("test", sch_tmp, &m);
	rc = msr_ldms_schema_add(msr, sch, &id_test);
	if (0 == rc) {
		printf("id: %s\n", id_test);
		free(id_test);
	} else {
		printf("error: %d\n", rc);
	}
}

void do_del_schema(msr_client_t msr, const char *del_id)
{
	int rc;
	rc = msr_ldms_schema_del(msr, del_id);
	if (0 == rc) {
		printf("id: %s\n", del_id);
	} else {
		printf("error: %d\n", rc);
	}
}

void do_list_names(msr_client_t msr)
{
	msr_results_t names, ids;
	int i, j;
	names = msr_names_list(msr);
	for (i = 0; i < names->n; i++) {
		printf("%s:\n", names->names[i]);
		ids = msr_ids_list(msr, names->names[i], NULL);
		for (j = 0; j < ids->n; j++) {
			printf(" - %s\n", ids->ids[j]);
		}
		free(ids);
	}
	free(names);
}

void do_list_digests(msr_client_t msr)
{
	msr_results_t digests, ids;
	int i, j;
	char buf[BUFSIZ];
	digests = msr_digests_list(msr);
	for (i = 0; i < digests->n; i++) {
		/* print digest */
		ldms_digest_str(&digests->digests[i], buf, sizeof(buf));
		printf("%s:\n", buf);
		ids = msr_ids_list(msr, NULL, &digests->digests[i]);
		for (j = 0; j < ids->n; j++) {
			printf(" - %s\n", ids->ids[j]);
		}
		free(ids);
	}
	free(digests);
}

void do_listen(msr_client_t msr, const char *id, const char *xprt,
		const char *host, const char *port)
{
	ldms_schema_t sch;
	ldms_t x;
	ldms_set_t lset;
	const char *name;
	char buf[BUFSIZ];
	int len;

	sch = msr_ldms_schema_get(msr, id);
	if (!sch) {
		printf("cannot get schema '%s', errno: %d\n", id, errno);
		return;
	}

	x = ldms_xprt_new(xprt);
	ldms_xprt_listen_by_name(x, host, port, NULL, NULL);

	gethostname(buf, sizeof(buf));
	len = strlen(buf);
	name = ldms_schema_name_get(sch);
	snprintf(buf + len, BUFSIZ - len, "/%s", name);
	lset = ldms_set_new(buf, sch);
	ldms_set_publish(lset);
	while (1) {
		sleep(1);
		ldms_transaction_begin(lset);
		ldms_transaction_end(lset);
	}
}

void set_op(char c, char *op)
{
	if (*op) {
		printf("already have operation '-%c',"
		       " but also got '-%c'\n", *op, c);
		exit(0);
	}
	*op = c;
}

int main(int argc, char **argv)
{
	msr_client_t msr;
	char c;
	char op = 0;
	int n_urls = 0;
	char *tok, *tmp;
	const char *urls[1024] = {0}; /* should be more than enough */
	char *arg_xprt = NULL;
	const char *_xprt = NULL, *_host = NULL, *_port = NULL;
	const char *arg_set_schema = NULL;
	const char *arg_ca_cert = NULL;
	const char *del_id = NULL;

	ldms_init(16*1024*1024);

 next_arg:
	c = getopt_long(argc, argv, short_opts, long_opts, NULL);
	switch (c) {
	case -1:
		goto arg_done;
	case 'a':
	case 'N':
	case 'D':
		set_op(c, &op);
		break;
	case 'd':
		set_op(c, &op);
		del_id = optarg;
		break;
	case 's':
		arg_set_schema = optarg;
		break;
	case 'x':
		set_op(c, &op);
		arg_xprt = optarg;
		break;
	case 'U':
		tok = strtok_r(optarg, ",", &tmp);
		while (tok) {
			urls[n_urls++] = tok;
			tok = strtok_r(NULL, ",", &tmp);
		}
		break;
	case 'C':
		arg_ca_cert = optarg;
		break;
	default:
		print_usage();
		exit(-1);
	}
	goto next_arg;

 arg_done:

	msr = msr_client_new(urls, arg_ca_cert);
	if (!msr) {
		printf("Cannot create registry client, errno: %d\n", errno);
		exit(-1);
	}

	switch (op) {
	case 'a':
		do_add_schema(msr);
		break;
	case 'd':
		do_del_schema(msr, del_id);
		break;
	case 'N':
		do_list_names(msr);
		break;
	case 'D':
		do_list_digests(msr);
		break;
	case 'x':
		_xprt = strtok_r(arg_xprt, ":", &tmp);
		_port = strtok_r(NULL, ":", &tmp);
		if (_port) {
			_host = strtok_r(NULL, ":", &tmp);
		}
		do_listen(msr, arg_set_schema, _xprt, _host, _port);
		break;
	}

	return 0;
}
