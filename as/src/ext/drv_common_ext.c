/*
 * drv_common_ext.c
 *
 * Ext build - cold start loads tombstones. Based on storage/drv_common_ce.c.
 *
 * Copyright (C) 2024 Aerospike, Inc.
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

#include "storage/drv_common.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "aerospike/as_atomic.h"

#include "arenax.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/set_index.h"
#include "storage/flat.h"
#include "transaction/rw_utils.h"

#include "ext_record.h"

//==========================================================
// Public API.
//

void
drv_adjust_sc_version_flags(as_namespace* ns, drv_pmeta* pmeta,
		bool wiped_drives, bool dirty)
{
	// Nothing to do - relevant for enterprise version only.
}

void
drv_mrt_create_cold_start_hash(as_namespace* ns)
{
}

// Might be better in a storage_ce.c if we ever do that.
bool
drv_load_needs_2nd_pass(const as_namespace* ns)
{
	return false;
}

bool
drv_mrt_2nd_pass_load_ticker(const as_namespace* ns, const char* pcts)
{
	return false;
}

void
drv_cold_start_remove_from_set_index(as_namespace* ns, as_index_tree* tree,
		as_index_ref* r_ref)
{
	as_set_index_delete_live(ns, tree, r_ref->r, r_ref->r_h);
}

// A flat record with no bins, or only bin tombstones (a bin cemetery), is a
// tombstone.
static bool
flat_is_tombstone(const as_flat_opt_meta* opt_meta)
{
	return opt_meta->n_bins == 0 || opt_meta->extra_flags.xdr_bin_cemetery == 1;
}

static void
set_cemetery(as_namespace* ns, as_index* r, bool is_cemetery)
{
	if ((r->xdr_bin_cemetery == 1) != is_cemetery) {
		r->xdr_bin_cemetery = is_cemetery ? 1 : 0;
		as_add_uint64(&ns->n_xdr_bin_cemeteries, is_cemetery ? 1 : -1);
	}
}

// A tombstone is indexed, but not in the set index. The caller has already
// assigned the set-id.
void
drv_cold_start_record_create(as_namespace* ns, const as_flat_record* flat,
		const as_flat_opt_meta* opt_meta, as_index_tree* tree,
		as_index_ref* r_ref)
{
	if (flat_is_tombstone(opt_meta)) {
		ext_record_set_tombstone(r_ref->r, ns, true);
		set_cemetery(ns, r_ref->r, opt_meta->n_bins != 0);
		r_ref->r->cenotaph = 1; // no older copy seen yet
		return;
	}

	as_set_index_insert(ns, tree, as_index_get_set_id(r_ref->r), r_ref->r_h);
}

// Called by the drivers' cold start update hooks, when a newer version of an
// indexed record is found. Moves it between live and tombstone as needed.
void
drv_cold_start_transition_tombstone(as_namespace* ns,
		const as_flat_opt_meta* opt_meta, as_index_tree* tree,
		as_index_ref* r_ref)
{
	as_index* r = r_ref->r;
	bool is_tombstone = flat_is_tombstone(opt_meta);

	set_cemetery(ns, r, is_tombstone && opt_meta->n_bins != 0);

	if (is_tombstone == (r->tombstone == 1)) {
		return;
	}

	if (is_tombstone) {
		as_set_index_delete_live(ns, tree, r, r_ref->r_h);
		ext_record_set_tombstone(r, ns, true);
		r->cenotaph = 0; // replaced an older live copy
	}
	else {
		ext_record_set_tombstone(r, ns, false);
		r->cenotaph = 0;
		as_set_index_insert(ns, tree, as_index_get_set_id(r), r_ref->r_h);
	}
}

// Reconcile the index element's set with the set named by the version we're
// about to apply. Cold start assigns set-id only when creating the element,
// which need not be the version carrying the set name. The element may be a
// tombstone here - the update hook then moves it between live and tombstone.
void
drv_cold_start_adopt_set(cf_log_context log_ctx, as_namespace* ns,
		const as_flat_record* flat, const as_flat_opt_meta* opt_meta,
		as_index_tree* tree, as_index_ref* r_ref)
{
	as_index* r = r_ref->r;

	if (drv_cold_start_set_already_assigned(log_ctx, ns, flat, opt_meta, r)) {
		return;
	}

	// Bounded set-name vmap - on failure leave the element setless, as it was.
	if (as_index_set_set_w_len(r, ns, opt_meta->set_name,
				opt_meta->set_name_len, false) != AS_OK) {
		return;
	}

	uint16_t set_id = as_index_get_set_id(r);

	if (r->tombstone == 1) {
		// Setting the set counted an object - count a tombstone instead.
		as_set* p_set = as_namespace_get_set_by_id(ns, set_id);

		as_namespace_release_set_id(ns, set_id);

		if (p_set != NULL) {
			as_incr_uint64(&p_set->n_tombstones);
		}
	}
	else {
		// The update path leaves the set index alone - record was already live.
		as_set_index_insert(ns, tree, set_id, r_ref->r_h);
	}

	// Charge the currently-indexed size - the caller's tail then applies its
	// (new - old) delta, so the set is charged the new size exactly once.
	as_namespace_adjust_set_data_used_bytes(ns, set_id,
			(int64_t)N_RBLOCKS_TO_SIZE(r->n_rblocks));
}

bool
drv_cold_start_sweeps_done(as_namespace* ns)
{
	return true;
}

as_record*
drv_current_record(as_namespace* ns, as_record* r, int file_id, uint64_t rblock_id)
{
	if (r->file_id == file_id && r->rblock_id == rblock_id) {
		return r;
	}

	return NULL;
}

uint32_t
drv_max_record_size(const as_namespace* ns, const as_record* r)
{
	return ns->max_record_size;
}
