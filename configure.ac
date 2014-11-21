dnl                                             -*- Autoconf -*-
AC_COPYRIGHT([
Copyright (c) 2014 Genome Research Ltd.
Author: Rob Davies <rmd+github@sanger.ac.uk>

This file is part of teepot.

Teepot is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses>])

dnl Process this file with autoconf to produce a configure script.

AC_PREREQ([2.68])
AC_INIT([teepot], [1.1.0], [])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([-Wall -Werror foreign])
AM_MAINTAINER_MODE
AC_CONFIG_SRCDIR([teepot.c])
AC_CONFIG_HEADERS([teepot_config.h])
AC_CONFIG_FILES([Makefile tests/Makefile])
AC_REQUIRE_AUX_FILE([tap-driver.sh])

# Checks for programs.
AC_PROG_CC
AC_PROG_AWK

# Checks for libraries.

# Checks for header files.
AC_HEADER_ASSERT
AC_CHECK_HEADERS([fcntl.h stdlib.h string.h unistd.h], [], [
echo "Missing required header file, giving up."
exit 1
])dnl

AC_CHECK_HEADERS([time.h sys/time.h])

# Check for large file support
AC_SYS_LARGEFILE

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T

# Checks for library functions.
AC_CHECK_FUNCS([memmove strerror strtoul], [], [
echo "Required function not found, giving up."
exit 1
])dnl

AC_SEARCH_LIBS([clock_gettime], [rt])

AC_CHECK_FUNCS([clock_gettime gettimeofday])

AC_OUTPUT
