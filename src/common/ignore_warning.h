// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * This can remove isolated warnings. For example, if there is a local
 * (i.e., static) function for debugging that is sometimes used but
 * not always, the macros START_UNUSED_FUNC and END_UNUSED_FUNC can
 * surround the definition to suppress the unused function warning.
 */

#include <boost/predef.h>

// GCC

#if BOOST_COMP_GNUC
#define START_UNUSED_FUNC _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wunused-function\"")

#define END_UNUSED_FUNC _Pragma("GCC diagnostic pop")
#endif

// clang

#if BOOST_COMP_CLANG
#define START_UNUSED_FUNC _Pragma("clang diagnostic push") _Pragma("clang diagnostic ignored \"-Wunused-function\"")

#define END_UNUSED_FUNC _Pragma("clang diagnostic pop")
#endif
