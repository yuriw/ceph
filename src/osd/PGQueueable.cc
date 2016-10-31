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

#include <sstream>

#include "PG.h"
#include "PGQueueable.h"
#include "OSD.h"
#include "common/debug.h"


static ostream& _prefix(std::ostream* _dout, int whoami, epoch_t epoch) {
  return *_dout << "osd." << whoami << " " << epoch << " ";
}

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, osd->whoami, osd->get_osdmap_epoch())


void PGQueueable::RunVis::operator()(const OpRequestRef &op) {
  static std::map<int,std::string> op_map;
  if (0 == op_map.size()) {
    op_map[42] = "CEPH_MSG_OSD_OP";
    op_map[94] = "MSG_OSD_PG_SCAN";
    op_map[95] = "MSG_OSD_PG_BACKFILL";
    op_map[105] = "MSG_OSD_PG_PUSH";
    op_map[106] = "MSG_OSD_PG_PULL";
    op_map[107] = "MSG_OSD_PG_PUSH_REPLY";
    op_map[112] = "MSG_OSD_REPOP";
    op_map[113] = "MSG_OSD_REPOPREPLY";
    op_map[114] = "MSG_OSD_PG_UPDATE_LOG_MISSING";
    op_map[115] = "MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY";
  }
  // TODO remove dout
  dout(0) << "OPQUEUE DEBUG executing dequeue_op" << dendl;
  // auto op_type = op->get_req()->get_header().type;
  auto op_type = op->get_req()->get_type();
  std::ostringstream op_out;
  op->get_req()->print(op_out);
  std::string op_name = op_map[op_type];
  static std::string unknown = "UNKNOWN";
  if ("" == op_name) op_name = unknown;
  dout(0) << "OPWATCH opval:" << op_type << " op:" << op_map[op_type] <<
    " " << op_out.str() << dendl;
  osd->dequeue_op(pg, op, handle);
}

void PGQueueable::RunVis::operator()(const PGSnapTrim &op) {
  // TODO remove dout
  dout(0) << "OPQUEUE DEBUG executing snap_trimmer" << dendl;
  pg->snap_trimmer(op.epoch_queued);
}

void PGQueueable::RunVis::operator()(const PGScrub &op) {
  // TODO remove dout
  dout(0) << "OPQUEUE DEBUG executing scrub" << dendl;
  pg->scrub(op.epoch_queued, handle);
}

void PGQueueable::RunVis::operator()(const PGRecovery &op) {
  // TODO remove dout
  dout(0) << "OPQUEUE DEBUG executing do_recovery" << dendl;
  osd->do_recovery(pg.get(), op.epoch_queued, op.reserved_pushes, handle);
}

void PGQueueable::DisplayVis::operator()(const OpRequestRef &op) {
  Message& msg = *boost::get<OpRequestRef>(op)->get_req();
  if (MSG_OSD_SUBOP == msg.get_header().type) {
    out << "osd-sub-op ";
  } else {
    out << "client-op, type:" << msg.get_type() <<
      ", tid:" << msg.get_tid() <<
      ", source_addr:" << msg.get_source_addr() <<
      ", source_inst:" << msg.get_source_inst() <<
      " ";
  }
}

void PGQueueable::DisplayVis::operator()(const PGSnapTrim &op) {
  out << "snaptrim ";
}

void PGQueueable::DisplayVis::operator()(const PGScrub &op) {
  out << "scrub ";
}

void PGQueueable::DisplayVis::operator()(const PGRecovery &op) {
  out << "recovery ";
}

std::ostream& operator<<(std::ostream& out, const PGQueueable& q) {
  PGQueueable::DisplayVis visitor(out);
  out << "req:{ owner:" << q.owner << ", priority:" << q.priority <<
    ", cost:" << q.cost << ", ";
  boost::apply_visitor(visitor, q.qvariant);
  out << "}";
  return out;
}
