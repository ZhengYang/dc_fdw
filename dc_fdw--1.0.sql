/* contrib/dc_fdw/dc_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION dc_fdw" to load this file. \quit

CREATE FUNCTION dc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION dc_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER dc_fdw
  HANDLER dc_fdw_handler
  VALIDATOR dc_fdw_validator;