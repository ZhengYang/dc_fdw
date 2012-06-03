/*-------------------------------------------------------------------------
 *
 * indexer.h
 *		  indexer for document collections foreign-data wrapper.
 *
 * Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence.
 *
 * Author: Zheng Yang <zhengyang4k@gmail.com>
 *
 * IDENTIFICATION
 *		  contrib/dc_fdw/indexer.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INDEXER_H
#define INDEXER_H

/* Debug mode flag */
#define DEBUG
#define NOT_USED
#define DC_F_BUFFER_SIZE 4*1024*1024 /* default buffer size for a file to be 4 MB ~= 4 million characters*/


#include "postgres.h"
#include "funcapi.h"
#include "storage/fd.h"
#include "tsearch/ts_utils.h"

#include "tsearch/ts_locale.h"
#include "tsearch/ts_public.h"
#include "utils/builtins.h"


/* Include the original Snowball header.h */
#include "snowball/libstemmer/header.h"
#include "snowball/libstemmer/stem_ISO_8859_1_danish.h"
#include "snowball/libstemmer/stem_ISO_8859_1_dutch.h"
#include "snowball/libstemmer/stem_ISO_8859_1_english.h"
#include "snowball/libstemmer/stem_ISO_8859_1_finnish.h"
#include "snowball/libstemmer/stem_ISO_8859_1_french.h"
#include "snowball/libstemmer/stem_ISO_8859_1_german.h"
#include "snowball/libstemmer/stem_ISO_8859_1_hungarian.h"
#include "snowball/libstemmer/stem_ISO_8859_1_italian.h"
#include "snowball/libstemmer/stem_ISO_8859_1_norwegian.h"
#include "snowball/libstemmer/stem_ISO_8859_1_porter.h"
#include "snowball/libstemmer/stem_ISO_8859_1_portuguese.h"
#include "snowball/libstemmer/stem_ISO_8859_1_spanish.h"
#include "snowball/libstemmer/stem_ISO_8859_1_swedish.h"
#include "snowball/libstemmer/stem_ISO_8859_2_romanian.h"
#include "snowball/libstemmer/stem_KOI8_R_russian.h"
#include "snowball/libstemmer/stem_UTF_8_danish.h"
#include "snowball/libstemmer/stem_UTF_8_dutch.h"
#include "snowball/libstemmer/stem_UTF_8_english.h"
#include "snowball/libstemmer/stem_UTF_8_finnish.h"
#include "snowball/libstemmer/stem_UTF_8_french.h"
#include "snowball/libstemmer/stem_UTF_8_german.h"
#include "snowball/libstemmer/stem_UTF_8_hungarian.h"
#include "snowball/libstemmer/stem_UTF_8_italian.h"
#include "snowball/libstemmer/stem_UTF_8_norwegian.h"
#include "snowball/libstemmer/stem_UTF_8_porter.h"
#include "snowball/libstemmer/stem_UTF_8_portuguese.h"
#include "snowball/libstemmer/stem_UTF_8_romanian.h"
#include "snowball/libstemmer/stem_UTF_8_russian.h"
#include "snowball/libstemmer/stem_UTF_8_spanish.h"
#include "snowball/libstemmer/stem_UTF_8_swedish.h"
#include "snowball/libstemmer/stem_UTF_8_turkish.h"




int dc_index(char* pathname);

#endif   /* INDEXER_H */