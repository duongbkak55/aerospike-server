/*
 * ext_xdr.h
 *
 * Ext build - XDR internals shared by xdr_ext.c (shipping) and
 * xdr_recovery_ext.c (last-ship-time and recovery).
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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "shash.h"

#include "base/xdr.h"


//==========================================================
// Forward declarations.
//

struct as_namespace_s;
struct xdr_dc_s;
struct xdr_recovery_s;


//==========================================================
// Typedefs & constants.
//

#define N_SHIP_WORKERS 4 // per DC

// Seconds of enqueue history tracked for the last-ship-time. An item older
// than this (destination down a long time) can't be tracked - the DC then
// flushes its queues and recovers from storage instead.
#define LST_RING_SZ 4096

// Queued-item flags, merged when a key is submitted again while queued.
#define QUEUED_DROP 0x01 // non-durable delete - ship a delete if gone
#define QUEUED_RECOVERY 0x02 // must ship all bins, counted in recovery

typedef struct ship_item_s {
	cf_digest keyd;
	uint64_t lut;
	uint32_t ns_ix;
	uint32_t enq_sec; // for the last-ship-time
	bool is_drop;
	bool is_recovery;
} ship_item;

typedef struct ship_worker_s {
	struct xdr_dc_s* dc;
	cf_queue* q; // of ship_item
	cf_mutex lock; // q pushes and 'queued' together
	cf_shash* queued; // digest -> flags, for keys in q
	int fd; // destination connection, -1 if none
	uint32_t seed_ix; // next seed to try
} ship_worker;

typedef struct dc_stats_s {
	uint64_t success;
	uint64_t not_found; // record gone (e.g. non-durable delete) before shipping
	uint64_t filtered_out;
	uint64_t abandoned; // destination rejected
	uint64_t retry_conn; // connection or send/receive failure
	uint64_t retry_dest; // destination returned a retryable error
	uint64_t lag_ms; // of the last shipped record
	uint64_t latency_us_total;
	uint64_t hot_keys; // submits merged into an already queued key
	uint64_t queue_overflows; // submits dropped - recovered from storage
	uint64_t recoveries; // recovery passes
	uint64_t recovered; // records queued by recovery
	uint64_t flushes; // queues flushed after a long outage
} dc_stats;

typedef struct xdr_dc_s {
	as_xdr_dc_cfg* cfg;
	uint32_t ix;
	char* credential; // NULL if no authentication
	ship_worker workers[N_SHIP_WORKERS];
	dc_stats stats;
	uint64_t prev_success; // for ticker throughput

	// Last-ship-time - all record versions with LUT <= lst have shipped.
	uint64_t lst;
	uint32_t pending[LST_RING_SZ]; // queued/in-flight items, by enqueue second
	uint32_t n_recovery_pending; // recovery items queued or in flight
	bool flushing; // workers drop items - recovery will ship them
	struct xdr_recovery_s* rec;
} xdr_dc;

typedef enum {
	ENQ_OK,
	ENQ_MERGED, // key already queued - that item ships the latest version
	ENQ_FULL
} enqueue_result;


//==========================================================
// Public API - xdr_ext.c.
//

enqueue_result ext_xdr_enqueue(xdr_dc* dc, uint32_t limit, ship_item* item);
uint32_t ext_xdr_queue_limit(const xdr_dc* dc, uint32_t ns_ix);
uint32_t ext_xdr_in_queue(const xdr_dc* dc);
bool ext_xdr_should_ship(const xdr_dc* dc, const struct as_namespace_s* ns,
		bool xdr_write, uint16_t set_id);


//==========================================================
// Public API - xdr_recovery_ext.c.
//

void ext_xdr_recovery_init(xdr_dc* dcs, uint32_t n_dcs);
void ext_xdr_recovery_start(void);
void ext_xdr_note_enqueued(xdr_dc* dc, ship_item* item);
void ext_xdr_note_done(xdr_dc* dc, const ship_item* item);
void ext_xdr_request_recovery_all(xdr_dc* dc);
void ext_xdr_recovery_stats(const xdr_dc* dc, cf_dyn_buf* db);
