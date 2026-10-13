/*
 * xdr_recovery_ext.c
 *
 * Ext build - XDR last-ship-time (LST) and recovery (P3c).
 *
 * A DC's LST promises that every record version with LUT <= LST has shipped.
 * It trails the oldest item still queued or in flight (tracked per enqueue
 * second), and is held back further while recovery is due or running.
 *
 * Recovery scans partitions and queues every record with LUT > a floor (an
 * LST). It runs when:
 * - A queue overflows - dropped writes are newer than the LST.
 * - The destination is down so long that items outlive the tracking window -
 *   queues are flushed and everything after the LST is recovered.
 * - This node becomes master of a partition - the previous master may not have
 *   shipped everything. The floor is the oldest LST published by the nodes of
 *   the previous cluster.
 * - The node restarts - its LST from before the restart holds until the
 *   partitions it masters have been recovered.
 *
 * Each node publishes its LST per DC in the XDR SMD module, which is shared by
 * the cluster and persisted, so LSTs survive restarts.
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

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "node.h"
#include "vector.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/smd.h"
#include "base/thr_info.h"
#include "fabric/exchange.h"
#include "fabric/partition.h"

#include "ext_xdr.h"


//==========================================================
// Typedefs & constants.
//

// Writes take far less than this from LUT assignment to XDR submit.
#define LST_MARGIN_MS 5000

#define TICK_US (1000 * 1000)
#define PUBLISH_PERIOD_MS 5000
#define PUBLISH_MIN_CHANGE_MS 1000

// Flush before items outlive the enqueue-second ring.
#define FLUSH_AGE_SEC (LST_RING_SZ - 120)

// The principal drops LSTs of nodes gone this long.
#define DEPARTED_DROP_MS (10 * 60 * 1000)

typedef struct lst_entry_s {
	uint32_t dc_ix;
	cf_node node;
	uint64_t lst;
	uint64_t absent_since; // principal only - 0 while in the cluster
} lst_entry;

typedef struct xdr_recovery_s {
	cf_mutex lock;

	// Recovery requests - floor per partition, 0 if none.
	uint64_t* req[AS_NAMESPACE_SZ];
	uint64_t req_min; // 0 if no partition requests
	uint64_t all_floor; // applies to every partition, 0 if none
	uint64_t active_floor; // pass in progress, 0 if none
	uint64_t startup_floor; // own LST before restart, until recovery due

	bool* was_master[AS_NAMESPACE_SZ]; // tick thread only
	uint32_t scan_from_sec; // tick thread only
	uint64_t published_lst; // tick thread only
	uint64_t published_ms; // tick thread only
} xdr_recovery;

typedef struct collect_info_s {
	const xdr_dc* dc;
	const as_namespace* ns;
	uint64_t floor;
	uint32_t now_void_time;
	cf_vector* items; // of ship_item
} collect_info;


//==========================================================
// Globals.
//

static xdr_dc* g_dcs;
static uint32_t g_n_dcs;

static cf_mutex g_table_lock = CF_MUTEX_INIT;
static cf_vector g_table; // of lst_entry - published LSTs

// Nodes that may have mastered partitions before the last cluster change.
// Tick thread only after startup.
static cf_vector g_prev_succession; // of cf_node


//==========================================================
// Forward declarations.
//

static void smd_accept_cb(const cf_vector* items, as_smd_accept_type type);
static void* run_tick(void* udata);
static void tick_dc(xdr_dc* dc, bool in_cluster);
static uint64_t compute_lst(xdr_dc* dc, uint64_t now_ms, uint32_t* oldest_r);
static void detect_gains(xdr_dc* dc);
static uint64_t gain_floor(const xdr_dc* dc);
static void publish_lst(xdr_dc* dc, uint64_t now_ms);
static void drop_departed(uint64_t now_ms);
static void* run_recovery(void* udata);
static void recover_partition(xdr_dc* dc, as_namespace* ns, uint32_t pid,
		uint64_t floor);
static bool collect_reduce_cb(as_index_ref* r_ref, void* udata);
static void request(xdr_recovery* rec, uint32_t ns_ix, uint32_t pid,
		uint64_t floor);
static lst_entry* find_entry(uint32_t dc_ix, cf_node node);
static int32_t find_dc_ix(const char* name, size_t len);


//==========================================================
// Inlines & macros.
//

static inline uint64_t
min_floor(uint64_t a, uint64_t b)
{
	return a == 0 ? b : b == 0 ? a : a < b ? a : b;
}

static inline bool
in_vector(const cf_vector* v, cf_node node)
{
	for (uint32_t i = 0; i < cf_vector_size(v); i++) {
		if (*(cf_node*)cf_vector_getp((cf_vector*)v, i) == node) {
			return true;
		}
	}

	return false;
}


//==========================================================
// Public API - ext_xdr.h.
//

void
ext_xdr_recovery_init(xdr_dc* dcs, uint32_t n_dcs)
{
	g_dcs = dcs;
	g_n_dcs = n_dcs;

	cf_vector_init(&g_table, sizeof(lst_entry), 16, 0);
	cf_vector_init(&g_prev_succession, sizeof(cf_node), AS_CLUSTER_SZ, 0);

	for (uint32_t i = 0; i < n_dcs; i++) {
		xdr_dc* dc = &dcs[i];
		xdr_recovery* rec = cf_calloc(1, sizeof(xdr_recovery));

		cf_mutex_init(&rec->lock);

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			if (dc->cfg->ns_cfgs[ns_ix] != NULL) {
				rec->req[ns_ix] = cf_calloc(AS_PARTITIONS, sizeof(uint64_t));
				rec->was_master[ns_ix] = cf_calloc(AS_PARTITIONS, sizeof(bool));
			}
		}

		dc->rec = rec;
	}

	// Restores the persisted LSTs - accept callback fills the table.
	as_smd_module_load(AS_SMD_MODULE_XDR, smd_accept_cb, NULL, NULL);

	// Before the restart, any node may have mastered what we'll master now.
	for (uint32_t i = 0; i < cf_vector_size(&g_table); i++) {
		lst_entry* e = cf_vector_getp(&g_table, i);

		if (! in_vector(&g_prev_succession, e->node)) {
			cf_vector_append(&g_prev_succession, &e->node);
		}

		if (e->node == g_config.self_node) {
			xdr_dc* dc = &dcs[e->dc_ix];

			// Our unshipped writes are newer than this - hold it until
			// recovery is requested for what we master.
			dc->rec->startup_floor = e->lst;
			dc->lst = e->lst;

			cf_info(AS_XDR, "DC %s: last-ship-time before restart %lu",
					dc->cfg->name, e->lst);
		}
	}
}

void
ext_xdr_recovery_start(void)
{
	if (g_n_dcs == 0) {
		return;
	}

	for (uint32_t i = 0; i < g_n_dcs; i++) {
		cf_thread_create_detached(run_recovery, &g_dcs[i]);
	}

	cf_thread_create_detached(run_tick, NULL);
}

// Called with the worker's lock held, just before the item is queued.
void
ext_xdr_note_enqueued(xdr_dc* dc, ship_item* item)
{
	item->enq_sec = (uint32_t)(cf_clepoch_milliseconds() / 1000);
	as_incr_uint32(&dc->pending[item->enq_sec % LST_RING_SZ]);
}

void
ext_xdr_note_done(xdr_dc* dc, const ship_item* item)
{
	as_decr_uint32(&dc->pending[item->enq_sec % LST_RING_SZ]);

	if (item->is_recovery) {
		as_decr_uint32(&dc->n_recovery_pending);
	}
}

// Writes are being dropped - all are newer than the current LST.
void
ext_xdr_request_recovery_all(xdr_dc* dc)
{
	xdr_recovery* rec = dc->rec;
	uint64_t floor = as_load_uint64(&dc->lst);

	if (floor == 0) {
		floor = 1; // no LST yet - recover everything
	}

	cf_mutex_lock(&rec->lock);
	rec->all_floor = min_floor(rec->all_floor, floor);
	cf_mutex_unlock(&rec->lock);
}

void
ext_xdr_recovery_stats(const xdr_dc* dc, cf_dyn_buf* db)
{
	xdr_recovery* rec = dc->rec;
	uint64_t lst = as_load_uint64(&dc->lst);
	uint64_t now = cf_clepoch_milliseconds();

	cf_mutex_lock(&rec->lock);

	bool recovering = rec->req_min != 0 || rec->all_floor != 0 ||
			rec->active_floor != 0;

	cf_mutex_unlock(&rec->lock);

	info_append_uint64(db, "lst", lst);
	info_append_uint64(db, "lst_lag", lst != 0 && lst < now ? now - lst : 0);
	info_append_bool(db, "recovering", recovering);
	info_append_uint32(db, "recovery_in_queue",
			as_load_uint32(&dc->n_recovery_pending));
	info_append_bool(db, "flushing", as_load_bool(&dc->flushing));
}


//==========================================================
// Local helpers - SMD.
//

// Keys are "<dc>|<node-id hex>", values the LST in ms.
static void
smd_accept_cb(const cf_vector* items, as_smd_accept_type type)
{
	cf_mutex_lock(&g_table_lock);

	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		const as_smd_item* item = cf_vector_get_ptr(items, i);
		const char* tok = strchr(item->key, TOK_DELIMITER);

		if (tok == NULL) {
			continue;
		}

		int32_t dc_ix = find_dc_ix(item->key, (size_t)(tok - item->key));

		if (dc_ix < 0) {
			continue; // DC not configured here (any more)
		}

		cf_node node = strtoull(tok + 1, NULL, 16);
		lst_entry* e = find_entry((uint32_t)dc_ix, node);

		if (item->value == NULL) {
			if (e != NULL) {
				cf_vector_delete(&g_table,
						(uint32_t)(e - (lst_entry*)cf_vector_getp(&g_table, 0)));
			}

			continue;
		}

		uint64_t lst = strtoull(item->value, NULL, 10);

		if (e != NULL) {
			e->lst = lst;
		}
		else {
			lst_entry ne = { .dc_ix = (uint32_t)dc_ix, .node = node, .lst = lst };

			cf_vector_append(&g_table, &ne);
		}
	}

	cf_mutex_unlock(&g_table_lock);
}


//==========================================================
// Local helpers - tick thread.
//

static void*
run_tick(void* udata)
{
	cf_vector succession;

	cf_vector_init(&succession, sizeof(cf_node), AS_CLUSTER_SZ, 0);

	while (true) {
		usleep(TICK_US);

		bool in_cluster = as_exchange_cluster_key() != 0;

		for (uint32_t i = 0; i < g_n_dcs; i++) {
			tick_dc(&g_dcs[i], in_cluster);
		}

		if (in_cluster) {
			cf_vector_clear(&succession);
			as_exchange_succession(&succession);

			if (cf_vector_size(&succession) != 0) {
				cf_vector_clear(&g_prev_succession);

				for (uint32_t i = 0; i < cf_vector_size(&succession); i++) {
					cf_vector_append(&g_prev_succession,
							cf_vector_getp(&succession, i));
				}
			}

			drop_departed(cf_clepoch_milliseconds());
		}
	}

	return NULL;
}

static void
tick_dc(xdr_dc* dc, bool in_cluster)
{
	xdr_recovery* rec = dc->rec;
	uint64_t now_ms = cf_clepoch_milliseconds();

	if (in_cluster) {
		detect_gains(dc);
	}

	uint32_t oldest;
	uint64_t lst = compute_lst(dc, now_ms, &oldest);

	cf_mutex_lock(&rec->lock);

	uint64_t floor = min_floor(min_floor(rec->req_min, rec->all_floor),
			min_floor(rec->active_floor, rec->startup_floor));

	if (floor != 0 && floor < lst) {
		lst = floor;
	}

	// Items outliving the ring can't be tracked - drop them all and recover
	// everything after the LST once the destination is back.
	if (oldest != 0 && now_ms / 1000 - oldest > FLUSH_AGE_SEC &&
			! as_load_bool(&dc->flushing)) {
		rec->all_floor = min_floor(rec->all_floor, lst);
		as_store_bool(&dc->flushing, true);
		as_incr_uint64(&dc->stats.flushes);

		cf_warning(AS_XDR, "DC %s: items queued too long - flushing queues, will recover from %lu",
				dc->cfg->name, lst);
	}

	cf_mutex_unlock(&rec->lock);

	if (oldest == 0 && as_load_bool(&dc->flushing)) {
		as_store_bool(&dc->flushing, false); // all flushed items done
	}

	as_store_uint64(&dc->lst, lst);

	if (in_cluster) {
		publish_lst(dc, now_ms);
	}
}

// Enqueue seconds only get new items at about 'now' - older buckets drain.
static uint64_t
compute_lst(xdr_dc* dc, uint64_t now_ms, uint32_t* oldest_r)
{
	xdr_recovery* rec = dc->rec;
	uint32_t now_sec = (uint32_t)(now_ms / 1000);
	uint32_t s = rec->scan_from_sec;

	if (s == 0 || now_sec - s >= LST_RING_SZ) {
		s = now_sec - (LST_RING_SZ - 1);
	}

	uint32_t oldest = 0;

	for (; s <= now_sec; s++) {
		if (as_load_uint32(&dc->pending[s % LST_RING_SZ]) != 0) {
			oldest = s;
			break;
		}
	}

	uint32_t settled = now_sec - 2;

	rec->scan_from_sec = oldest != 0 && oldest < settled ? oldest : settled;
	*oldest_r = oldest;

	return (oldest != 0 ? (uint64_t)oldest * 1000 : now_ms) - LST_MARGIN_MS;
}

// A partition we now master may hold writes the previous master never
// shipped - recover it from the oldest LST of the previous cluster's nodes.
static void
detect_gains(xdr_dc* dc)
{
	xdr_recovery* rec = dc->rec;
	uint64_t floor = 0;
	bool any_balanced = false;

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		bool* was = rec->was_master[ns_ix];

		if (was == NULL) {
			continue;
		}

		as_namespace* ns = g_config.namespaces[ns_ix];

		for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
			cf_node master = ns->partitions[pid].replicas[0];
			bool is = master == g_config.self_node;

			if (master != (cf_node)0) {
				any_balanced = true;
			}

			if (is && ! was[pid]) {
				if (floor == 0) {
					floor = gain_floor(dc);

					if (floor == 0) {
						floor = UINT64_MAX; // no LSTs published - nothing to do
					}
				}

				if (floor != UINT64_MAX) {
					request(rec, ns_ix, pid, floor);
				}
			}

			was[pid] = is;
		}
	}

	// Partitions balanced - the requests now cover what we master.
	if (any_balanced && rec->startup_floor != 0) {
		cf_mutex_lock(&rec->lock);
		rec->startup_floor = 0;
		cf_mutex_unlock(&rec->lock);
	}
}

static uint64_t
gain_floor(const xdr_dc* dc)
{
	uint64_t floor = 0;

	cf_mutex_lock(&g_table_lock);

	for (uint32_t i = 0; i < cf_vector_size(&g_table); i++) {
		lst_entry* e = cf_vector_getp(&g_table, i);

		if (e->dc_ix == dc->ix && (e->node == g_config.self_node ||
				in_vector(&g_prev_succession, e->node))) {
			floor = min_floor(floor, e->lst);
		}
	}

	cf_mutex_unlock(&g_table_lock);

	return floor;
}

static void
publish_lst(xdr_dc* dc, uint64_t now_ms)
{
	xdr_recovery* rec = dc->rec;
	uint64_t lst = as_load_uint64(&dc->lst);

	if (now_ms - rec->published_ms < PUBLISH_PERIOD_MS ||
			(rec->published_lst != 0 && lst >= rec->published_lst &&
					lst - rec->published_lst < PUBLISH_MIN_CHANGE_MS)) {
		return;
	}

	char key[128];
	char value[32];

	snprintf(key, sizeof(key), "%s%c%lx", dc->cfg->name, TOK_DELIMITER,
			g_config.self_node);
	snprintf(value, sizeof(value), "%lu", lst);

	as_smd_set_and_forget(AS_SMD_MODULE_XDR, key, value);

	rec->published_lst = lst;
	rec->published_ms = now_ms;
}

// LSTs of nodes long gone only hold back recovery after a full restart.
static void
drop_departed(uint64_t now_ms)
{
	if (as_exchange_principal() != g_config.self_node) {
		return;
	}

	cf_mutex_lock(&g_table_lock);

	for (uint32_t i = 0; i < cf_vector_size(&g_table); i++) {
		lst_entry* e = cf_vector_getp(&g_table, i);

		if (in_vector(&g_prev_succession, e->node)) {
			e->absent_since = 0;
			continue;
		}

		if (e->absent_since == 0) {
			e->absent_since = now_ms;
			continue;
		}

		if (now_ms - e->absent_since > DEPARTED_DROP_MS) {
			char key[128];

			snprintf(key, sizeof(key), "%s%c%lx", g_dcs[e->dc_ix].cfg->name,
					TOK_DELIMITER, e->node);
			e->absent_since = now_ms; // until the delete is accepted

			as_smd_delete_and_forget(AS_SMD_MODULE_XDR, key);
		}
	}

	cf_mutex_unlock(&g_table_lock);
}


//==========================================================
// Local helpers - recovery thread.
//

static void*
run_recovery(void* udata)
{
	xdr_dc* dc = (xdr_dc*)udata;
	xdr_recovery* rec = dc->rec;
	uint64_t* snap[AS_NAMESPACE_SZ] = { 0 };

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		if (rec->req[ns_ix] != NULL) {
			snap[ns_ix] = cf_malloc(AS_PARTITIONS * sizeof(uint64_t));
		}
	}

	while (true) {
		usleep(TICK_US);

		if (as_load_bool(&dc->flushing)) {
			continue;
		}

		cf_mutex_lock(&rec->lock);

		if (rec->req_min == 0 && rec->all_floor == 0) {
			cf_mutex_unlock(&rec->lock);
			continue;
		}

		uint64_t all_floor = rec->all_floor;

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			if (snap[ns_ix] != NULL) {
				memcpy(snap[ns_ix], rec->req[ns_ix],
						AS_PARTITIONS * sizeof(uint64_t));
				memset(rec->req[ns_ix], 0, AS_PARTITIONS * sizeof(uint64_t));
			}
		}

		rec->active_floor = min_floor(rec->req_min, all_floor);
		rec->req_min = 0;
		rec->all_floor = 0;

		uint64_t active_floor = rec->active_floor;

		cf_mutex_unlock(&rec->lock);

		as_incr_uint64(&dc->stats.recoveries);

		uint64_t start_recovered = as_load_uint64(&dc->stats.recovered);
		uint64_t start_ms = cf_getms();

		cf_info(AS_XDR, "DC %s: recovery from %lu started", dc->cfg->name,
				active_floor);

		for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
			if (snap[ns_ix] == NULL) {
				continue;
			}

			as_namespace* ns = g_config.namespaces[ns_ix];

			for (uint32_t pid = 0; pid < AS_PARTITIONS; pid++) {
				uint64_t floor = min_floor(snap[ns_ix][pid], all_floor);

				if (floor != 0 && ! as_load_bool(&dc->flushing)) {
					recover_partition(dc, ns, pid, floor);
				}
			}
		}

		// The floor holds until everything recovery queued has shipped.
		while (as_load_uint32(&dc->n_recovery_pending) != 0 &&
				! as_load_bool(&dc->flushing)) {
			usleep(100 * 1000);
		}

		cf_mutex_lock(&rec->lock);
		rec->active_floor = 0;
		cf_mutex_unlock(&rec->lock);

		cf_info(AS_XDR, "DC %s: recovery from %lu %s - %lu records in %lu ms",
				dc->cfg->name, active_floor,
				as_load_bool(&dc->flushing) ? "interrupted" : "done",
				as_load_uint64(&dc->stats.recovered) - start_recovered,
				cf_getms() - start_ms);
	}

	return NULL;
}

// Only the final master recovers a partition, once migrations into it are
// done - records arriving by migration aren't submitted to XDR.
static void
recover_partition(xdr_dc* dc, as_namespace* ns, uint32_t pid, uint64_t floor)
{
	as_partition* p = &ns->partitions[pid];

	while (true) {
		if (p->replicas[0] != g_config.self_node) {
			return; // the new master recovers it
		}

		if (as_load_uint16(&p->pending_immigrations) == 0) {
			break;
		}

		if (as_load_bool(&dc->flushing)) {
			return;
		}

		usleep(TICK_US);
	}

	cf_vector items;

	cf_vector_init(&items, sizeof(ship_item), 64, 0);

	collect_info info = {
			.dc = dc,
			.ns = ns,
			.floor = floor,
			.now_void_time = as_record_void_time_get(),
			.items = &items
	};

	as_partition_reservation rsv;

	as_partition_reserve(ns, pid, &rsv);
	as_index_reduce(rsv.tree, collect_reduce_cb, &info);
	as_partition_release(&rsv);

	// Leave half of each queue for live writes.
	uint32_t limit = ext_xdr_queue_limit(dc, ns->ix);
	uint32_t room = limit / 2;

	for (uint32_t i = 0; i < cf_vector_size(&items); i++) {
		ship_item* item = cf_vector_getp(&items, i);
		uint32_t w_ix;

		memcpy(&w_ix, item->keyd.digest, sizeof(w_ix));

		const ship_worker* w = &dc->workers[w_ix % N_SHIP_WORKERS];

		while (true) {
			if (as_load_bool(&dc->flushing)) {
				cf_vector_destroy(&items);
				return;
			}

			if (cf_queue_sz(w->q) < (int)room &&
					ext_xdr_enqueue(dc, limit, item) != ENQ_FULL) {
				break;
			}

			usleep(10 * 1000);
		}

		as_incr_uint64(&dc->stats.recovered);
	}

	cf_vector_destroy(&items);
}

// Tombstones are recovered too - they ship as deletes.
static bool
collect_reduce_cb(as_index_ref* r_ref, void* udata)
{
	collect_info* info = (collect_info*)udata;
	as_record* r = r_ref->r;

	if (r->last_update_time > info->floor &&
			(r->void_time == 0 || r->void_time > info->now_void_time) &&
			ext_xdr_should_ship(info->dc, info->ns, r->xdr_write == 1,
					as_index_get_set_id(r))) {
		ship_item item = {
				.keyd = r->keyd,
				.lut = r->last_update_time,
				.ns_ix = info->ns->ix,
				.is_recovery = true
		};

		cf_vector_append(info->items, &item);
	}

	as_record_done(r_ref, (as_namespace*)info->ns);

	return true;
}

static void
request(xdr_recovery* rec, uint32_t ns_ix, uint32_t pid, uint64_t floor)
{
	cf_mutex_lock(&rec->lock);

	rec->req[ns_ix][pid] = min_floor(rec->req[ns_ix][pid], floor);
	rec->req_min = min_floor(rec->req_min, floor);

	cf_mutex_unlock(&rec->lock);
}


//==========================================================
// Local helpers - generic.
//

// Called with g_table_lock held.
static lst_entry*
find_entry(uint32_t dc_ix, cf_node node)
{
	for (uint32_t i = 0; i < cf_vector_size(&g_table); i++) {
		lst_entry* e = cf_vector_getp(&g_table, i);

		if (e->dc_ix == dc_ix && e->node == node) {
			return e;
		}
	}

	return NULL;
}

static int32_t
find_dc_ix(const char* name, size_t len)
{
	for (uint32_t i = 0; i < g_n_dcs; i++) {
		const char* dc_name = g_dcs[i].cfg->name;

		if (strlen(dc_name) == len && memcmp(dc_name, name, len) == 0) {
			return (int32_t)i;
		}
	}

	return -1;
}
