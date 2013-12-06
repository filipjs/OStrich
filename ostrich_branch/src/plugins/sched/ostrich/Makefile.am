# Makefile for ostrich scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

INCLUDES = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = sched_ostrich.la

sched_ostrich_la_SOURCES = \
			ostrich_wrapper.c \
			ostrich.c	\
			ostrich.h	\
			../backfill/backfill.c	\
			../backfill/backfill.h
sched_ostrich_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
