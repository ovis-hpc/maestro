OVIS_PREFIX ?= /opt/ovis

ifdef OVIS_PREFIX
	OVIS_INCLUDES = -I$(OVIS_PREFIX)/include
	OVIS_LIBS = -L$(OVIS_PREFIX)/lib -L$(OVIS_PREFIX)/lib64
else
	OVIS_INCLUDES =
	OVIS_LIBS =
endif

CFLAGS = -ggdb3 -O0 $(OVIS_INCLUDES)
LDFLAGS = $(OVIS_LIBS) -lldms -lcurl -ljansson

all: libmsr_client.so msr_test

libmsr_client_so_SOURCES = msr_client.c

libmsr_client.so: $(libmsr_client_so_SOURCES) Makefile
	$(CC) $(libmsr_client_so_SOURCES) $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) -shared -o $@

msr_test: msr_test.c libmsr_client.so Makefile
	$(CC) $< $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) -L. -lmsr_client -o $@

clean:
	rm -f libmsr_client.so msr_test


install: libmsr_client.so msr_client.h msr_test
	LIBPREFIX=$$(ldd libmsr_client.so | grep libldms.so | \
		  		sed 's/.* => \(\S\+\)\/libldms.so.*/\1/' \
			); \
	install -t $${LIBPREFIX}/ libmsr_client.so ; \
	install -t $${LIBPREFIX}/../bin msr_test
