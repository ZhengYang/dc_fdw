# contrib/dc_fdw/Makefile

# module built from multiple source files
MODULE_big = dc_fdw
OBJS = indexer.o dc_fdw.o \
../../src/backend/snowball/api.o \
../../src/backend/snowball/utilities.o \
../../src/backend/snowball/stem_ISO_8859_1_danish.o \
../../src/backend/snowball/stem_ISO_8859_1_dutch.o \
../../src/backend/snowball/stem_ISO_8859_1_english.o \
../../src/backend/snowball/stem_ISO_8859_1_finnish.o \
../../src/backend/snowball/stem_ISO_8859_1_french.o \
../../src/backend/snowball/stem_ISO_8859_1_german.o \
../../src/backend/snowball/stem_ISO_8859_1_hungarian.o \
../../src/backend/snowball/stem_ISO_8859_1_italian.o \
../../src/backend/snowball/stem_ISO_8859_1_norwegian.o \
../../src/backend/snowball/stem_ISO_8859_1_porter.o \
../../src/backend/snowball/stem_ISO_8859_1_portuguese.o \
../../src/backend/snowball/stem_ISO_8859_1_spanish.o \
../../src/backend/snowball/stem_ISO_8859_1_swedish.o \
../../src/backend/snowball/stem_ISO_8859_2_romanian.o \
../../src/backend/snowball/stem_KOI8_R_russian.o \
../../src/backend/snowball/stem_UTF_8_danish.o \
../../src/backend/snowball/stem_UTF_8_dutch.o \
../../src/backend/snowball/stem_UTF_8_english.o \
../../src/backend/snowball/stem_UTF_8_finnish.o \
../../src/backend/snowball/stem_UTF_8_french.o \
../../src/backend/snowball/stem_UTF_8_german.o \
../../src/backend/snowball/stem_UTF_8_hungarian.o \
../../src/backend/snowball/stem_UTF_8_italian.o \
../../src/backend/snowball/stem_UTF_8_norwegian.o \
../../src/backend/snowball/stem_UTF_8_porter.o \
../../src/backend/snowball/stem_UTF_8_portuguese.o \
../../src/backend/snowball/stem_UTF_8_romanian.o \
../../src/backend/snowball/stem_UTF_8_russian.o \
../../src/backend/snowball/stem_UTF_8_spanish.o \
../../src/backend/snowball/stem_UTF_8_swedish.o \
../../src/backend/snowball/stem_UTF_8_turkish.o

EXTENSION = dc_fdw
DATA = dc_fdw--1.0.sql

REGRESS = dc_fdw

#EXTRA_CLEAN = sql/dc_fdw.sql expected/dc_fdw.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dc_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
