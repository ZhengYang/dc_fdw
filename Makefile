# contrib/dc_fdw/Makefile

# module built from multiple source files
MODULE_big = dc_fdw
OBJS = indexer.o searcher.o qual_extract.o dc_fdw.o

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
