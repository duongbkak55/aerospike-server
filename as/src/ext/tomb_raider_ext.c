/*
 * tomb_raider_ext.c
 *
 * Ext build - removes tombstones once they're old enough and no older live
 * copy of their record remains on storage (which cold start could resurrect).
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

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "cf_thread.h"
#include "log.h"

#include "base/datamodel.h"
#include "base/index.h"
#include "fabric/partition.h"
#include "storage/drv_common.h"
#include "storage/flat.h"
#include "storage/storage.h"

#include "ext_record.h"


//==========================================================
// Typedefs & constants.
//

typedef struct raider_s {
	as_namespace* ns;
	ext_tomb_raider_scan_fn scan; // NULL if storage doesn't persist
} raider;

typedef struct pass_info_s {
	as_namespace* ns;
	as_index_tree* tree; // current partition's tree
	uint64_t cutoff_lut;
	uint64_t n_tombstones;
	uint64_t n_marked;
	uint64_t n_removed;
} pass_info;


//==========================================================
// Forward declarations.
//

static uint64_t eligible_cutoff_lut(const as_namespace* ns);
static void* run_tomb_raider(void* udata);
static bool migrations_pending(const as_namespace* ns);
static void reduce_all_partitions(as_namespace* ns, as_index_reduce_fn cb,
		pass_info* info);
static bool mark_reduce_cb(as_index_ref* r_ref, void* udata);
static bool remove_reduce_cb(as_index_ref* r_ref, void* udata);


//==========================================================
// Ext API - see ext_record.h.
//

void
ext_tomb_raider_start(as_namespace* ns, ext_tomb_raider_scan_fn scan)
{
	if (ns->tomb_raider_period == 0) {
		cf_info(AS_NSUP, "{%s} tomb raider disabled (tomb-raider-period 0)",
				ns->name);
		return;
	}

	// Accepted for config compatibility - the scan is one thread per namespace
	// and namespaces aren't serialized.
	if (ns->n_tomb_raider_unmark_threads != 0) {
		cf_warning(AS_NSUP, "{%s} tomb-raider-unmark-threads is ignored",
				ns->name);
	}

	if (ns->storage_serialize_tomb_raider) {
		cf_warning(AS_NSUP, "{%s} serialize-tomb-raider is ignored", ns->name);
	}

	raider* rdr = cf_malloc(sizeof(raider));

	rdr->ns = ns;
	rdr->scan = scan;

	cf_info(AS_NSUP, "{%s} starting tomb raider - period %u eligible-age %u",
			ns->name, ns->tomb_raider_period, ns->tomb_raider_eligible_age);

	cf_thread_create_detached(run_tomb_raider, rdr);
}

// Called by the storage scan for every record found on storage. A live copy of
// a record now marked for removal is older than its tombstone - removing the
// tombstone would let cold start resurrect the copy, so unmark it.
void
ext_tomb_raider_check_flat(as_namespace* ns, const as_flat_record* flat)
{
	if (flat->has_bins == 0) {
		return; // older tombstone copies are harmless
	}

	as_partition_reservation rsv;

	as_partition_reserve(ns, as_partition_getid(&flat->keyd), &rsv);

	as_index_ref r_ref;

	if (as_record_get(rsv.tree, &flat->keyd, &r_ref) == 0) {
		as_record* r = r_ref.r;

		if (r->tombstone == 1 && r->cenotaph == 1) {
			r->cenotaph = 0;
		}

		as_record_done(&r_ref, ns);
	}

	as_partition_release(&rsv);
}


void
ext_cold_start_adjust_cenotaph(const as_flat_record* flat, as_record* r)
{
	if (r->tombstone == 1 && flat->has_bins == 1) {
		r->cenotaph = 0;
	}
}

// Same rule as the tomb raider's remove pass - only tombstones past the
// eligible age go, so recent ones still beat older copies on other nodes.
void
ext_cold_start_drop_cenotaphs(as_namespace* ns)
{
	pass_info info = { .ns = ns, .cutoff_lut = eligible_cutoff_lut(ns) };

	reduce_all_partitions(ns, remove_reduce_cb, &info);

	if (info.n_removed != 0) {
		cf_info(AS_NSUP, "{%s} cold start dropped %lu tombstones with no older copies",
				ns->name, info.n_removed);
	}
}

// Walks a wblock the way cold start does - records start with the flat magic
// and are skipped by size, anything else is skipped an rblock at a time.
void
ext_tomb_raider_check_wblock(as_namespace* ns, const uint8_t* buf)
{
	uint32_t indent = 0;

	while (indent + sizeof(as_flat_record) <= WBLOCK_SZ) {
		const as_flat_record* flat = (const as_flat_record*)&buf[indent];

		if (flat->magic != AS_FLAT_MAGIC) {
			if (indent == 0) {
				return; // unused wblock
			}

			indent += RBLOCK_SIZE;
			continue;
		}

		uint32_t record_size = N_RBLOCKS_TO_SIZE(flat->n_rblocks);

		if (record_size < DRV_RECORD_MIN_SIZE ||
				indent + record_size > WBLOCK_SZ) {
			indent += RBLOCK_SIZE;
			continue;
		}

		ext_tomb_raider_check_flat(ns, flat);
		indent += record_size;
	}
}

//==========================================================
// Local helpers.
//

static void*
run_tomb_raider(void* udata)
{
	raider* rdr = (raider*)udata;
	as_namespace* ns = rdr->ns;

	while (true) {
		sleep(ns->tomb_raider_period);

		if (as_load_uint64(&ns->n_tombstones) == 0) {
			continue;
		}

		// A tombstone must outlive migrations - a node bringing an older copy
		// back must find it.
		if (migrations_pending(ns)) {
			cf_info(AS_NSUP, "{%s} tomb raider skipped - migrations pending",
					ns->name);
			continue;
		}

		uint64_t start_ms = cf_getms();
		pass_info info = { .ns = ns, .cutoff_lut = eligible_cutoff_lut(ns) };

		// 1. Mark eligible tombstones.
		reduce_all_partitions(ns, mark_reduce_cb, &info);

		if (info.n_marked == 0) {
			cf_info(AS_NSUP, "{%s} tomb raider: %lu tombstones, none eligible",
					ns->name, info.n_tombstones);
			continue;
		}

		// 2. Unmark those with older live copies on storage.
		if (rdr->scan != NULL) {
			rdr->scan(ns);
		}

		if (migrations_pending(ns)) {
			cf_info(AS_NSUP, "{%s} tomb raider stopped - migrations started",
					ns->name);
			continue; // next mark pass resets the marks
		}

		// 3. Remove the rest.
		reduce_all_partitions(ns, remove_reduce_cb, &info);

		cf_info(AS_NSUP, "{%s} tomb raider: %lu tombstones, %lu eligible, %lu removed in %lu ms",
				ns->name, info.n_tombstones, info.n_marked, info.n_removed,
				cf_getms() - start_ms);
	}

	return NULL;
}

// Tombstones with a LUT below this are eligible for removal.
static uint64_t
eligible_cutoff_lut(const as_namespace* ns)
{
	uint64_t now = cf_clepoch_milliseconds();
	uint64_t age_ms = (uint64_t)ns->tomb_raider_eligible_age * 1000;

	return age_ms < now ? now - age_ms : 0;
}

static bool
migrations_pending(const as_namespace* ns)
{
	return as_load_uint64(&ns->migrate_tx_partitions_remaining) != 0 ||
			as_load_uint64(&ns->migrate_rx_partitions_remaining) != 0;
}

static void
reduce_all_partitions(as_namespace* ns, as_index_reduce_fn cb, pass_info* info)
{
	for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
		as_partition_reservation rsv;

		as_partition_reserve(ns, pid, &rsv);
		info->tree = rsv.tree;
		as_index_reduce(rsv.tree, cb, info);
		as_partition_release(&rsv);
	}
}

// Marks are reset every cycle, so stale marks from an earlier cycle can't
// cause a removal.
static bool
mark_reduce_cb(as_index_ref* r_ref, void* udata)
{
	pass_info* info = (pass_info*)udata;
	as_record* r = r_ref->r;

	if (r->tombstone == 1) {
		info->n_tombstones++;

		if (r->last_update_time < info->cutoff_lut) {
			r->cenotaph = 1;
			info->n_marked++;
		}
		else {
			r->cenotaph = 0;
		}
	}
	else {
		r->cenotaph = 0;
	}

	as_record_done(r_ref, info->ns);

	return true;
}

// A record revived (or deleted again) since the mark pass has a newer LUT, so
// the cutoff check excludes it even if its mark is stale.
static bool
remove_reduce_cb(as_index_ref* r_ref, void* udata)
{
	pass_info* info = (pass_info*)udata;
	as_record* r = r_ref->r;

	if (r->tombstone == 1 && r->cenotaph == 1 &&
			r->last_update_time < info->cutoff_lut) {
		// Tombstones are in neither the set index nor sindexes. Dropping the
		// index element frees its storage and adjusts the tombstone counters.
		as_index_delete(info->tree, &r->keyd);
		info->n_removed++;
	}
	else {
		r->cenotaph = 0;
	}

	as_record_done(r_ref, info->ns);

	return true;
}
