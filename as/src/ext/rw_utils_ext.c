/*
 * rw_utils_ext.c
 *
 * Ext build - read/write hooks with durable delete (tombstone) support and
 * bin convergence (conflict-resolve-writes). Based on
 * transaction/rw_utils_ce.c.
 *
 * Copyright (C) 2016-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 */

//==========================================================
// Includes.
//

#include "transaction/rw_utils.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "citrusleaf/alloc.h"

#include "log.h"
#include "msg.h"
#include "vector.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/transaction.h"
#include "storage/storage.h"
#include "transaction/rw_request.h"

#include "ext_record.h"

//==========================================================
// Typedefs & constants.
//

// Bin convergence - per-transaction state. Set by prepare_bin_metadata() and
// forbid_resolve(), which run on the transaction's thread before its ops.
typedef struct resolve_ctx_s {
	uint64_t old_lut; // record LUT before this write, 0 if new
	bool old_binless_tombstone; // all bins were deleted at old_lut
	bool xdr; // XDR write to a conflict-resolve-writes namespace
	uint64_t msg_lut; // packed - see ext_xdr_pack_lut()
} resolve_ctx;

static __thread resolve_ctx tl_rc;


//==========================================================
// Forward declarations.
//

static bool incoming_wins(const as_bin* b, uint64_t packed_lut);
static void set_resolved_meta(as_storage_rd* rd, as_bin* b,
		uint64_t packed_lut);
static uint64_t op_packed_lut(const as_msg_op* op, uint64_t msg_lut);


//==========================================================
// Public API.
//

// A durable delete becomes a write with a single delete-all op, so the write
// path stores, replicates and indexes a tombstone (a record with no bins).
// The message is in host byte order here.
static cl_msg*
make_durable_delete_write(const cl_msg* msgp)
{
	size_t old_sz = sizeof(as_proto) + msgp->proto.sz;
	cl_msg* new_msgp = cf_malloc(old_sz + sizeof(as_msg_op));

	memcpy(new_msgp, msgp, old_sz);

	as_msg_op* op = (as_msg_op*)((uint8_t*)new_msgp + old_sz);

	*op = (as_msg_op){ .op_sz = OP_FIXED_SZ, .op = AS_MSG_OP_DELETE_ALL };

	new_msgp->proto.sz += sizeof(as_msg_op);
	new_msgp->msg.n_ops++;
	new_msgp->msg.info2 &= ~AS_MSG_INFO2_DELETE;
	new_msgp->msg.info2 |= AS_MSG_INFO2_DURABLE_DELETE;

	// Tombstones don't expire - also avoids TTL checks (e.g. ttl disallowed
	// with nsup-period 0) rejecting the delete.
	new_msgp->msg.record_ttl = TTL_NEVER_EXPIRE;

	return new_msgp;
}

// With bin convergence every delete is durable - a delete must leave LUTs
// behind, or an older write arriving later would bring the record back.
static bool
must_convert(const as_namespace* ns, const as_transaction* tr)
{
	return as_transaction_is_durable_delete(tr) ||
			(ns->conflict_resolve_writes && as_transaction_is_delete(tr));
}

bool
convert_to_write(as_transaction* tr, cl_msg** p_msgp)
{
	// Shared messages (batch) were converted by convert_batched_to_write().
	if (! must_convert(tr->rsv.ns, tr) || SHARED_MSGP(tr)) {
		return false;
	}

	cl_msg* new_msgp = make_durable_delete_write(tr->msgp);

	cf_free(tr->msgp);
	tr->msgp = new_msgp;
	*p_msgp = new_msgp;

	return true;
}

// Batch sub-transaction messages live in the batch buffer - converted copies
// are owned by the batch, and freed by destroy_batch_extra_msgps().
void
convert_batched_to_write(const as_namespace* ns, as_transaction* tr,
		void** p_extra_msgps)
{
	if (! must_convert(ns, tr)) {
		return;
	}

	if (*p_extra_msgps == NULL) {
		*p_extra_msgps = cf_vector_create(sizeof(cl_msg*), 8, 0);
	}

	cl_msg* new_msgp = make_durable_delete_write(tr->msgp);

	cf_vector_append_ptr((cf_vector*)*p_extra_msgps, new_msgp);
	tr->msgp = new_msgp;
}

void
destroy_batch_extra_msgps(void* extra_msgps)
{
	cf_vector* v = (cf_vector*)extra_msgps;

	if (v == NULL) {
		return;
	}

	for (uint32_t i = 0; i < cf_vector_size(v); i++) {
		cf_free(cf_vector_get_ptr(v, i));
	}

	cf_vector_destroy(v);
}

int
validate_delete_durability(as_transaction* tr)
{
	return AS_OK;
}

int
repl_state_check(as_record* r, as_transaction* tr)
{
	return 0;
}

void
will_replicate(as_record* r, as_namespace* ns)
{
}

bool
write_is_full_drop(const as_transaction* tr)
{
	return IS_DROP(tr);
}

bool
sufficient_replica_destinations(const as_namespace* ns, uint32_t n_dests)
{
	return true;
}

bool
set_replica_destinations(as_transaction* tr, rw_request* rw)
{
	rw->n_dest_nodes = as_partition_get_other_replicas(tr->rsv.p, rw->dest_nodes);

	return true;
}

void
finished_replicated(as_transaction* tr)
{
}

void
finished_not_replicated(rw_request* rw)
{
}

bool
generation_check(const as_record* r, const as_msg* m, const as_namespace* ns)
{
	if ((m->info2 & AS_MSG_INFO2_GENERATION) != 0) {
		return m->generation == r->generation;
	}

	if ((m->info2 & AS_MSG_INFO2_GENERATION_GT) != 0) {
		return m->generation > r->generation;
	}

	return true; // no generation requirement
}

// Bins missing from a replace must leave LUTs behind - not supported.
bool
forbid_replace(const as_namespace* ns)
{
	return ns->conflict_resolve_writes;
}

void
prepare_bin_metadata(const as_transaction* tr, as_storage_rd* rd)
{
	as_record* r = rd->r;

	rd->bin_luts = rd->ns->conflict_resolve_writes;

	tl_rc.old_lut = r->generation != 0 ? r->last_update_time : 0;
	tl_rc.old_binless_tombstone = r->generation != 0 && r->tombstone == 1 &&
			r->xdr_bin_cemetery == 0;
	tl_rc.xdr = false;
	tl_rc.msg_lut = 0;

	if (rd->bin_luts) {
		for (uint32_t i = 0; i < rd->n_bins; i++) {
			as_bin* b = &rd->bins[i];

			// Preserve LUT shared with record in case bin is not written.
			if (b->lut == 0) {
				b->lut = r->last_update_time;
			}
		}

		return;
	}

	// Remove all metadata.
	for (uint32_t i = 0; i < rd->n_bins; i++) {
		rd->bins[i].lut = 0;
	}
}

void
unwind_index_metadata(const as_record* old_r, as_record* r)
{
	r->void_time = old_r->void_time;
	r->last_update_time = old_r->last_update_time;
	r->generation = old_r->generation;
	r->tombstone = old_r->tombstone;
	r->xdr_bin_cemetery = old_r->xdr_bin_cemetery;
	r->xdr_write = old_r->xdr_write;
}

void
set_xdr_write(const as_transaction* tr, as_record* r)
{
	// XDR doesn't ship records last written by XDR unless 'forward' is set -
	// this is what stops active-active loops.
	r->xdr_write = as_transaction_is_xdr(tr) ? 1 : 0;
}

// A touch re-stamps live bins with the new record LUT. Bin tombstones keep
// theirs.
void
touch_bin_metadata(as_storage_rd* rd)
{
	if (rd->bin_luts) {
		for (uint32_t i = 0; i < rd->n_bins; i++) {
			as_bin* b = &rd->bins[i];

			if (as_bin_is_live(b)) {
				b->lut = 0;
				b->src_id = 0;
				b->xdr_write = 0;
			}
		}
	}
}

// Called before the record is written to storage. A durable delete leaves a
// non-expiring tombstone, as does a write leaving only bin tombstones (a bin
// cemetery) - any other write makes the record live again.
void
transition_delete_metadata(as_transaction* tr, as_record* r, bool is_delete,
		bool is_bin_cemetery)
{
	if (! is_delete) {
		r->tombstone = 0;
		r->xdr_bin_cemetery = 0;
		return;
	}

	if (is_bin_cemetery || as_transaction_is_durable_delete(tr)) {
		r->tombstone = 1;
		r->xdr_bin_cemetery = is_bin_cemetery ? 1 : 0;
		r->void_time = 0;
	}
}

// Called at the start of a write's ops. Only notes the XDR context here.
bool
forbid_resolve(const as_transaction* tr, const as_storage_rd* rd, uint64_t msg_lut)
{
	tl_rc.xdr = rd->ns->conflict_resolve_writes && as_transaction_is_xdr(tr);
	tl_rc.msg_lut = msg_lut;

	return false;
}

// An XDR write op applies only if it is newer than the bin it would change -
// by LUT, then by src-id. Losing every op loses the whole write.
bool
resolve_bin(as_storage_rd* rd, const as_msg_op* op, uint64_t msg_lut,
		uint16_t n_ops, uint16_t* n_won, int* result)
{
	if (! tl_rc.xdr || op->op != AS_MSG_OP_WRITE) {
		return true;
	}

	as_bin* b = as_bin_get_w_len(rd, op->name, op->name_sz);

	if (incoming_wins(b, op_packed_lut(op, msg_lut))) {
		return true;
	}

	*result = --(*n_won) == 0 ? AS_ERR_LOST_CONFLICT : AS_OK;

	return false;
}

bool
udf_resolve_bin(as_storage_rd* rd, const char* name)
{
	return true;
}

// With bin convergence a deleted bin becomes a tombstone carrying the
// delete's LUT - including a bin not (yet) here, which a write shipped from
// elsewhere may still bring.
void
delete_bin(as_storage_rd* rd, const as_msg_op* op, uint64_t msg_lut)
{
	if (! rd->ns->conflict_resolve_writes) {
		as_bin_delete_w_len(rd, op->name, op->name_sz);
		return;
	}

	as_bin* b = as_bin_get_or_create_w_len(rd, op->name, op->name_sz);

	as_bin_set_tombstone(b);

	if (tl_rc.xdr) {
		set_resolved_meta(rd, b, op_packed_lut(op, msg_lut));
	}
}

void
udf_delete_bin(as_storage_rd* rd, const char* name)
{
	if (! rd->ns->conflict_resolve_writes) {
		as_bin_delete(rd, name);
		return;
	}

	as_bin* b = as_bin_get_or_create(rd, name);

	as_bin_set_tombstone(b);
}

// The bin was just (re)created - meta is clear. XDR writes keep the shipped
// bin's LUT and src-id.
void
write_resolved_bin(as_storage_rd* rd, const as_msg_op* op, uint64_t msg_lut,
		as_bin* b)
{
	if (tl_rc.xdr) {
		set_resolved_meta(rd, b, op_packed_lut(op, msg_lut));
	}
}

// Caller has already handled destroying all bins' particles. With bin
// convergence live bins become tombstones - for an XDR delete only those older
// than the delete.
void
delete_all_bins(as_storage_rd* rd)
{
	if (! rd->ns->conflict_resolve_writes) {
		rd->n_bins = 0;
		return;
	}

	bool xdr = tl_rc.xdr && ext_xdr_lut(tl_rc.msg_lut) != 0;

	for (uint32_t i = 0; i < rd->n_bins; i++) {
		as_bin* b = &rd->bins[i];

		if (! as_bin_is_live(b)) {
			continue; // existing tombstones keep their LUTs
		}

		if (xdr && ! incoming_wins(b, tl_rc.msg_lut)) {
			continue;
		}

		as_bin_set_tombstone(b);

		if (xdr) {
			set_resolved_meta(rd, b, tl_rc.msg_lut);
		}
		else {
			b->lut = 0; // the new record LUT
			b->src_id = 0;
			b->xdr_write = 0;
		}
	}
}

//==========================================================
// Private API - for enterprise separation only.
//

// Tombstones stay in the index - other deletes drop the record.
void
write_delete_record(as_record* r, as_index_tree* tree)
{
	if (r->tombstone == 0) {
		as_index_delete(tree, &r->keyd);
	}
}

uint32_t
dup_res_pack_repl_state_info(const as_record* r, const as_namespace* ns)
{
	return 0;
}

bool
dup_res_should_retry_transaction(rw_request* rw, uint32_t result_code)
{
	return false;
}

void
dup_res_handle_tie(rw_request* rw, const msg* m, uint32_t result_code)
{
}

void
apply_if_tie(rw_request* rw)
{
}

void
dup_res_translate_result_code(rw_request* rw)
{
	rw->result_code = AS_OK;
}

void
dup_res_init_repl_state(as_remote_record* rr, uint32_t info)
{
}

void
repl_write_add_regime(msg* m, const as_transaction* tr)
{
}

void
repl_write_init_repl_state(as_remote_record* rr, bool from_replica)
{
}

conflict_resolution_pol
repl_write_conflict_resolution_policy(const as_namespace* ns)
{
	return AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_LAST_UPDATE_TIME;
}

bool
repl_write_should_retransmit_replicas(rw_request* rw, uint32_t result_code)
{
	switch (result_code) {
	case AS_ERR_CLUSTER_KEY_MISMATCH:
		rw->xmit_ms = 0; // force retransmit on next cycle
		return true;
	default:
		return false;
	}
}

void
repl_write_with_orig(rw_request* rw)
{
}

void
repl_write_send_confirmation(rw_request* rw)
{
}

void
repl_write_handle_confirmation(msg* m)
{
}

int
record_replace_check(as_record* r, as_namespace* ns)
{
	return 0;
}

void
record_replaced(as_record* r, as_remote_record* rr)
{
}


//==========================================================
// Local helpers - bin convergence.
//

static uint64_t
op_packed_lut(const as_msg_op* op, uint64_t msg_lut)
{
	return op->has_lut == 1 ? as_msg_op_get_lut(op) : msg_lut;
}

// Bins hold LUT 0 and src-id 0 for "same as the record" and "this cluster".
// A bin that isn't here is older than anything - unless the record was a
// tombstone, whose LUT then applies to every bin.
static bool
incoming_wins(const as_bin* b, uint64_t packed_lut)
{
	uint64_t lut = ext_xdr_lut(packed_lut);

	if (lut == 0) {
		return true; // sender has no LUTs - plain overwrite
	}

	uint8_t src_id = ext_xdr_src_id(packed_lut);
	uint64_t cur_lut;
	uint8_t cur_src_id = g_config.xdr_cfg.src_id;

	if (b != NULL) {
		cur_lut = b->lut != 0 ? b->lut : tl_rc.old_lut;

		if (b->src_id != 0) {
			cur_src_id = b->src_id;
		}
	}
	else if (tl_rc.old_binless_tombstone) {
		cur_lut = tl_rc.old_lut;
	}
	else {
		return true;
	}

	return lut > cur_lut || (lut == cur_lut && src_id > cur_src_id);
}

// The record LUT never falls behind its bins' LUTs - so a later local write,
// stamped with a newer record LUT, beats every bin it overwrites.
static void
set_resolved_meta(as_storage_rd* rd, as_bin* b, uint64_t packed_lut)
{
	uint64_t lut = ext_xdr_lut(packed_lut);

	if (lut == 0) {
		return; // sender has no LUTs - treat as a local write
	}

	b->lut = lut;
	b->src_id = ext_xdr_src_id(packed_lut);
	b->xdr_write = 1;

	if (rd->r->last_update_time < lut) {
		rd->r->last_update_time = lut;
	}
}
