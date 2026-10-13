/*
 * xdr_ext.c
 *
 * Ext build - XDR shipping (P3a). Replaces base/xdr_ce.c.
 *
 * Writes on a partition's master are submitted to a per-DC worker chosen by
 * digest (so a key's changes ship in order). The worker reads the record's
 * current version and sends it to the destination cluster as an XDR write -
 * or a durable delete for a tombstone, or a non-durable delete for a record
 * dropped by a non-durable delete. Writes that arrived via XDR aren't
 * shipped on unless the DC namespace has 'forward' set, which prevents loops
 * in active-active setups.
 *
 * Not yet (P3b/P3c): bin convergence, recovery after queue overflow or
 * restart, destination partition map, compression, TLS, filters.
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

#include "base/xdr.h"

#include <arpa/inet.h>
#include <crypt.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"
#include "citrusleaf/cf_clock.h"
#include "citrusleaf/cf_digest.h"
#include "citrusleaf/cf_queue.h"

#include "cf_mutex.h"
#include "cf_thread.h"
#include "dynbuf.h"
#include "log.h"
#include "vector.h"

#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/index.h"
#include "base/proto.h"
#include "base/security.h"
#include "base/thr_info.h"
#include "base/transaction.h"
#include "fabric/partition.h"
#include "storage/storage.h"

#include "ext_record.h"


//==========================================================
// Typedefs & constants.
//

#define N_SHIP_WORKERS 4 // per DC
#define DEFAULT_QUEUE_LIMIT (16 * 1024) // per DC namespace
#define SOCKET_TIMEOUT_MS 5000
#define RETRY_SLEEP_MS 500
#define TRANSACTION_TTL_MS 5000

// Clients' fixed bcrypt salt - credentials are hashed with it.
#define CLIENT_BCRYPT_SALT "$2a$10$7EqJtq98hPqEX7fNZaFWoO"

// Security protocol bits used to log in to the destination.
#define SEC_CMD_LOGIN 20
#define SEC_FIELD_USER 0
#define SEC_FIELD_CREDENTIAL 3

typedef struct dc_seed_s {
	char* host;
	char* port;
	char* tls_name;
} dc_seed;

typedef struct ship_item_s {
	cf_digest keyd;
	uint64_t lut;
	uint32_t ns_ix;
	bool is_drop; // non-durable delete - ship a delete if the record is gone
} ship_item;

struct xdr_dc_s;

typedef struct ship_worker_s {
	struct xdr_dc_s* dc;
	cf_queue* q; // of ship_item
	int fd; // destination connection, -1 if none
	uint32_t seed_ix; // next seed to try
} ship_worker;

typedef struct dc_stats_s {
	uint64_t success;
	uint64_t not_found; // record gone (e.g. non-durable delete) before shipping
	uint64_t filtered_out;
	uint64_t abandoned; // queue full or destination rejected
	uint64_t retry_conn; // connection or send/receive failure
	uint64_t retry_dest; // destination returned a retryable error
	uint64_t lag_ms; // of the last shipped record
	uint64_t latency_us_total;
} dc_stats;

typedef struct xdr_dc_s {
	as_xdr_dc_cfg* cfg;
	char* credential; // NULL if no authentication
	ship_worker workers[N_SHIP_WORKERS];
	dc_stats stats;
	uint64_t prev_success; // for ticker throughput
} xdr_dc;

typedef enum {
	BUILD_OK,
	BUILD_NOT_FOUND,
	BUILD_SKIP
} build_result;


//==========================================================
// Globals.
//

static xdr_dc g_dcs[AS_XDR_MAX_DCS];
static uint32_t g_n_dcs = 0;


//==========================================================
// Forward declarations.
//

static void link_dc(xdr_dc* dc);
static char* load_credential(const as_xdr_dc_cfg* cfg);
static bool set_shipped(const as_xdr_dc_ns_cfg* ns_cfg, const as_namespace* ns,
		uint16_t set_id);
static void* run_ship_worker(void* udata);
static void ship_item_until_done(ship_worker* w, const ship_item* item);
static void submit(const as_namespace* ns, const as_xdr_submit_info* info,
		bool is_drop);
static build_result build_ship_msg(const as_namespace* ns,
		const as_xdr_dc_ns_cfg* ns_cfg, const ship_item* item, cf_dyn_buf* db);
static void build_drop_msg(const as_namespace* ns,
		const as_xdr_dc_ns_cfg* ns_cfg, const cf_digest* keyd, cf_dyn_buf* db);
static void finish_proto(cf_dyn_buf* db);
static int send_and_receive(ship_worker* w, const uint8_t* buf, size_t sz,
		uint8_t* result_code);
static bool ensure_connected(ship_worker* w);
static void disconnect(ship_worker* w);
static int connect_seed(const dc_seed* seed);
static bool login(int fd, const xdr_dc* dc);
static bool send_all(int fd, const uint8_t* buf, size_t sz);
static bool recv_all(int fd, uint8_t* buf, size_t sz);
static bool recv_proto(int fd, uint8_t type, uint8_t** body, size_t* sz);
static void append_field(cf_dyn_buf* db, uint8_t type, const void* data,
		uint32_t sz);
static xdr_dc* find_dc(const char* name);


//==========================================================
// Inlines & macros.
//

static inline ship_worker*
worker_for(xdr_dc* dc, const cf_digest* keyd)
{
	uint32_t h;

	memcpy(&h, keyd->digest, sizeof(h));

	return &dc->workers[h % N_SHIP_WORKERS];
}

static inline bool
is_master(const as_namespace* ns, const cf_digest* keyd)
{
	const as_partition* p = &ns->partitions[as_partition_getid(keyd)];

	return p->replicas[0] == g_config.self_node;
}


//==========================================================
// Public API - generic.
//

void
as_xdr_init(void)
{
	for (uint32_t i = 0; i < g_n_dcs; i++) {
		link_dc(&g_dcs[i]);
	}
}

void
as_xdr_start(void)
{
	for (uint32_t i = 0; i < g_n_dcs; i++) {
		xdr_dc* dc = &g_dcs[i];

		for (uint32_t w = 0; w < N_SHIP_WORKERS; w++) {
			cf_thread_create_detached(run_ship_worker, &dc->workers[w]);
		}

		cf_info(AS_XDR, "DC %s: started %u ship workers", dc->cfg->name,
				N_SHIP_WORKERS);
	}
}


//==========================================================
// Public API - manager.
//

as_xdr_dc_cfg*
as_xdr_startup_create_dc(const char* dc_name)
{
	if (g_n_dcs == AS_XDR_MAX_DCS) {
		cf_crash_nostack(AS_XDR, "too many DCs - max %u", AS_XDR_MAX_DCS);
	}

	if (find_dc(dc_name) != NULL) {
		cf_crash_nostack(AS_XDR, "DC %s configured twice", dc_name);
	}

	as_xdr_dc_cfg* cfg = cf_calloc(1, sizeof(as_xdr_dc_cfg));

	cfg->name = cf_strdup(dc_name);
	cfg->auth_mode = XDR_AUTH_NONE;
	cfg->max_recoveries_interleaved = 0;
	cfg->n_recovery_threads = 1;
	cfg->period_us = 100 * 1000;
	cfg->ns_cfg_v = cf_vector_create(sizeof(as_xdr_dc_ns_cfg*), 4, 0);

	cf_mutex_init(&cfg->seed_lock);
	cf_vector_init(&cfg->seed_nodes, sizeof(dc_seed), 4, 0);

	g_dcs[g_n_dcs++].cfg = cfg;

	return cfg;
}

as_xdr_dc_ns_cfg*
as_xdr_startup_create_dc_ns_cfg(const char* ns_name)
{
	as_xdr_dc_ns_cfg* ns_cfg = cf_calloc(1, sizeof(as_xdr_dc_ns_cfg));

	ns_cfg->ns_name = cf_strdup(ns_name);
	ns_cfg->bin_policy = XDR_BIN_POLICY_ALL;
	ns_cfg->compression_level = 1;
	ns_cfg->compression_threshold = AS_XDR_MIN_COMPRESSION_THRESHOLD;
	ns_cfg->hot_key_ms = 100;
	ns_cfg->sc_replication_wait_ms = 100;
	ns_cfg->ship_versions_policy = XDR_SHIP_VERSIONS_POLICY_LATEST;
	ns_cfg->transaction_queue_limit = DEFAULT_QUEUE_LIMIT;
	ns_cfg->write_policy = XDR_WRITE_POLICY_AUTO;

	ns_cfg->ignored_sets = cf_vector_create(sizeof(char*), 4, 0);
	ns_cfg->shipped_sets = cf_vector_create(sizeof(char*), 4, 0);
	ns_cfg->ignored_bins = cf_vector_create(sizeof(char*), 4, 0);
	ns_cfg->shipped_bins = cf_vector_create(sizeof(char*), 4, 0);

	return ns_cfg;
}

void
as_xdr_startup_add_seed(as_xdr_dc_cfg* cfg, char* host, char* port,
		char* tls_name)
{
	dc_seed seed = { .host = host, .port = port, .tls_name = tls_name };

	cf_vector_append(&cfg->seed_nodes, &seed);
}

void
as_xdr_link_tls(void)
{
	// No TLS in this build - link_dc() rejects 'tls-name'.
}

as_xdr_ship_status
as_xdr_ship_check(const as_record* r, as_transaction* tr)
{
	return XDR_SHIP_OK; // no hot-key handling yet
}

void
as_xdr_get_submit_info(const as_record* r, uint64_t prev_lut,
		as_xdr_submit_info* info)
{
	info->keyd = r->keyd;
	info->lut = r->last_update_time;
	info->prev_lut = prev_lut;
	info->set_id = as_index_get_set_id(r);
	info->xdr_write = r->xdr_write == 1;
	info->xdr_tombstone = r->xdr_tombstone == 1;
	info->xdr_nsup_tombstone = r->xdr_nsup_tombstone == 1;
}

// Called after every write - on masters (ship) and replicas (ignored here).
void
as_xdr_submit(const as_namespace* ns, const as_xdr_submit_info* info)
{
	submit(ns, info, false);
}

// Called by the ext delete path after a non-durable delete on the master.
void
ext_xdr_submit_drop(const as_namespace* ns, const as_xdr_submit_info* info)
{
	submit(ns, info, true);
}

static void
submit(const as_namespace* ns, const as_xdr_submit_info* info, bool is_drop)
{
	if (g_n_dcs == 0 || ! is_master(ns, &info->keyd)) {
		return;
	}

	for (uint32_t i = 0; i < g_n_dcs; i++) {
		xdr_dc* dc = &g_dcs[i];
		const as_xdr_dc_ns_cfg* ns_cfg = dc->cfg->ns_cfgs[ns->ix];

		if (ns_cfg == NULL) {
			continue;
		}

		if ((info->xdr_write && ! ns_cfg->forward) ||
				! set_shipped(ns_cfg, ns, info->set_id)) {
			as_incr_uint64(&dc->stats.filtered_out);
			continue;
		}

		ship_worker* w = worker_for(dc, &info->keyd);
		uint32_t limit = ns_cfg->transaction_queue_limit / N_SHIP_WORKERS;

		if (cf_queue_sz(w->q) >= (int)limit) {
			// TODO - P3c: recovery from last-ship-time instead of dropping.
			as_incr_uint64(&dc->stats.abandoned);
			cf_ticker_warning(AS_XDR, "DC %s {%s}: queue full - abandoning",
					dc->cfg->name, ns->name);
			continue;
		}

		ship_item item = {
				.keyd = info->keyd, .lut = info->lut, .ns_ix = ns->ix,
				.is_drop = is_drop
		};

		cf_queue_push(w->q, &item);
	}
}

void
as_xdr_ticker(uint64_t delta_time)
{
	for (uint32_t i = 0; i < g_n_dcs; i++) {
		xdr_dc* dc = &g_dcs[i];
		dc_stats* s = &dc->stats;
		uint64_t success = as_load_uint64(&s->success);
		uint32_t in_queue = 0;

		for (uint32_t w = 0; w < N_SHIP_WORKERS; w++) {
			in_queue += (uint32_t)cf_queue_sz(dc->workers[w].q);
		}

		cf_info(AS_XDR, "DC %s: in-queue %u success (%lu,%.1f) abandoned %lu not-found %lu filtered-out %lu retry (%lu,%lu) lag %lu ms",
				dc->cfg->name, in_queue, success,
				delta_time == 0 ? 0.0 : (double)(success - dc->prev_success) *
						1000 / (double)delta_time,
				as_load_uint64(&s->abandoned), as_load_uint64(&s->not_found),
				as_load_uint64(&s->filtered_out),
				as_load_uint64(&s->retry_conn), as_load_uint64(&s->retry_dest),
				as_load_uint64(&s->lag_ms));

		dc->prev_success = success;
	}
}

void
as_xdr_cleanup_tl_stats(void)
{
}


//==========================================================
// Public API - reader & ship (EE's service-thread shipping - unused here).
//

void
as_xdr_read(as_transaction* tr)
{
	cf_warning(AS_XDR, "unexpected internal XDR transaction");
}

void
as_xdr_init_poll(cf_poll poll)
{
}

void
as_xdr_shutdown_poll(void)
{
}

void
as_xdr_io_event(uint32_t mask, void* data)
{
}

void
as_xdr_timer_event(cf_poll_event* events, int32_t n_events, uint32_t e_ix)
{
}


//==========================================================
// Public API - info.
//

void
as_xdr_get_config(const char* cmd, cf_dyn_buf* db)
{
	char dc_name[64];
	int len = (int)sizeof(dc_name);

	if (as_info_parameter_get(cmd, "dc", dc_name, &len) != 0) {
		info_append_uint32(db, "src-id", g_config.xdr_cfg.src_id);
		cf_dyn_buf_append_string(db, "dcs=");

		for (uint32_t i = 0; i < g_n_dcs; i++) {
			cf_dyn_buf_append_string(db, g_dcs[i].cfg->name);
			cf_dyn_buf_append_char(db, ',');
		}

		cf_dyn_buf_chomp_char(db, ',');
		cf_dyn_buf_append_char(db, ';');
		return;
	}

	xdr_dc* dc = find_dc(dc_name);

	if (dc == NULL) {
		as_info_respond_error(db, AS_ERR_NOT_FOUND, "DC not found");
		return;
	}

	cf_dyn_buf_append_string(db, "node-address-port=");

	for (uint32_t i = 0; i < cf_vector_size(&dc->cfg->seed_nodes); i++) {
		const dc_seed* seed = cf_vector_getp(&dc->cfg->seed_nodes, i);

		cf_dyn_buf_append_format(db, "%s:%s,", seed->host, seed->port);
	}

	cf_dyn_buf_chomp_char(db, ',');
	cf_dyn_buf_append_string(db, ";namespaces=");

	for (uint32_t i = 0; i < g_config.n_namespaces; i++) {
		if (dc->cfg->ns_cfgs[i] != NULL) {
			cf_dyn_buf_append_string(db, g_config.namespaces[i]->name);
			cf_dyn_buf_append_char(db, ',');
		}
	}

	cf_dyn_buf_chomp_char(db, ',');
	cf_dyn_buf_append_char(db, ';');
	info_append_bool(db, "auth-mode-internal",
			dc->cfg->auth_mode == XDR_AUTH_INTERNAL);
}

bool
as_xdr_set_config(const char* cmd)
{
	return false; // dynamic DC config not supported yet
}

void
as_xdr_get_stats(const char* cmd, cf_dyn_buf* db)
{
	char dc_name[64];
	int len = (int)sizeof(dc_name);

	if (as_info_parameter_get(cmd, "dc", dc_name, &len) != 0) {
		as_info_respond_error(db, AS_ERR_PARAMETER, "missing dc");
		return;
	}

	xdr_dc* dc = find_dc(dc_name);

	if (dc == NULL) {
		as_info_respond_error(db, AS_ERR_NOT_FOUND, "DC not found");
		return;
	}

	dc_stats* s = &dc->stats;
	uint32_t in_queue = 0;

	for (uint32_t w = 0; w < N_SHIP_WORKERS; w++) {
		in_queue += (uint32_t)cf_queue_sz(dc->workers[w].q);
	}

	uint64_t success = as_load_uint64(&s->success);

	info_append_uint32(db, "in_queue", in_queue);
	info_append_uint64(db, "success", success);
	info_append_uint64(db, "not_found", as_load_uint64(&s->not_found));
	info_append_uint64(db, "filtered_out", as_load_uint64(&s->filtered_out));
	info_append_uint64(db, "abandoned", as_load_uint64(&s->abandoned));
	info_append_uint64(db, "retry_conn_reset", as_load_uint64(&s->retry_conn));
	info_append_uint64(db, "retry_dest", as_load_uint64(&s->retry_dest));
	info_append_uint64(db, "lag", as_load_uint64(&s->lag_ms));
	info_append_uint64(db, "latency_us", success == 0 ? 0 :
			as_load_uint64(&s->latency_us_total) / success);

	cf_dyn_buf_chomp_char(db, ';');
}

void
as_xdr_dc_state(as_info_cmd_args* args)
{
	as_info_respond_error(args->db, AS_ERR_UNKNOWN, "not supported");
}

void
as_xdr_get_filter(as_info_cmd_args* args)
{
	as_info_respond_error(args->db, AS_ERR_UNKNOWN, "not supported");
}

void
as_xdr_set_filter(as_info_cmd_args* args)
{
	as_info_respond_error(args->db, AS_ERR_UNKNOWN, "not supported");
}


//==========================================================
// Local helpers - startup.
//

static void
link_dc(xdr_dc* dc)
{
	as_xdr_dc_cfg* cfg = dc->cfg;

	if (cf_vector_size(&cfg->seed_nodes) == 0) {
		cf_crash_nostack(AS_XDR, "DC %s has no node-address-port", cfg->name);
	}

	if (cfg->tls_our_name != NULL) {
		cf_crash_nostack(AS_XDR, "DC %s: tls-name not supported - no TLS in this build",
				cfg->name);
	}

	if (cfg->connector) {
		cf_crash_nostack(AS_XDR, "DC %s: connector not supported yet",
				cfg->name);
	}

	if (cfg->auth_mode != XDR_AUTH_NONE && cfg->auth_mode != XDR_AUTH_INTERNAL) {
		cf_crash_nostack(AS_XDR, "DC %s: only auth-mode none or internal is supported",
				cfg->name);
	}

	dc->credential = cfg->auth_mode == XDR_AUTH_INTERNAL ?
			load_credential(cfg) : NULL;

	for (uint32_t i = 0; i < cf_vector_size(cfg->ns_cfg_v); i++) {
		as_xdr_dc_ns_cfg* ns_cfg = cf_vector_get_ptr(cfg->ns_cfg_v, i);
		as_namespace* ns = as_namespace_get_byname(ns_cfg->ns_name);

		if (ns == NULL) {
			cf_crash_nostack(AS_XDR, "DC %s: unknown namespace %s", cfg->name,
					ns_cfg->ns_name);
		}

		if (ns_cfg->bin_policy != XDR_BIN_POLICY_ALL || ns_cfg->ship_bin_luts) {
			cf_crash_nostack(AS_XDR, "DC %s {%s}: only bin-policy all is supported yet (P3b)",
					cfg->name, ns->name);
		}

		if (ns_cfg->compression_enabled ||
				cf_vector_size(ns_cfg->shipped_bins) != 0 ||
				cf_vector_size(ns_cfg->ignored_bins) != 0) {
			cf_crash_nostack(AS_XDR, "DC %s {%s}: compression and bin filters not supported yet",
					cfg->name, ns->name);
		}

		if (ns_cfg->transaction_queue_limit < N_SHIP_WORKERS) {
			ns_cfg->transaction_queue_limit = DEFAULT_QUEUE_LIMIT;
		}

		cfg->ns_cfgs[ns->ix] = ns_cfg;

		cf_info(AS_XDR, "DC %s {%s}: shipping to namespace %s%s", cfg->name,
				ns->name, ns_cfg->remote_namespace != NULL ?
						ns_cfg->remote_namespace : ns->name,
				ns_cfg->forward ? " (forward)" : "");
	}

	for (uint32_t w = 0; w < N_SHIP_WORKERS; w++) {
		dc->workers[w] = (ship_worker){
				.dc = dc,
				.q = cf_queue_create(sizeof(ship_item), true),
				.fd = -1,
				.seed_ix = w
		};
	}
}

static char*
load_credential(const as_xdr_dc_cfg* cfg)
{
	if (cfg->auth_user == NULL || cfg->auth_password_file == NULL) {
		cf_crash_nostack(AS_XDR, "DC %s: auth-mode internal needs auth-user and auth-password-file",
				cfg->name);
	}

	FILE* f = fopen(cfg->auth_password_file, "r");
	char password[256];

	if (f == NULL || fgets(password, sizeof(password), f) == NULL) {
		cf_crash_nostack(AS_XDR, "DC %s: can't read auth-password-file %s",
				cfg->name, cfg->auth_password_file);
	}

	fclose(f);
	password[strcspn(password, "\r\n")] = '\0';

	struct crypt_data data;

	memset(&data, 0, sizeof(data));

	const char* hash = crypt_r(password, CLIENT_BCRYPT_SALT, &data);

	memset(password, 0, sizeof(password));

	if (hash == NULL || hash[0] == '*') {
		cf_crash_nostack(AS_XDR, "DC %s: can't hash password", cfg->name);
	}

	char* credential = cf_strdup(hash);

	memset(&data, 0, sizeof(data));

	return credential;
}

// Set names are compared rather than set-ids - sets named in config may not
// exist yet at startup.
static bool
set_shipped(const as_xdr_dc_ns_cfg* ns_cfg, const as_namespace* ns,
		uint16_t set_id)
{
	const char* set_name = as_namespace_get_set_name(ns, set_id);
	const cf_vector* list = ns_cfg->ship_only_specified_sets ?
			ns_cfg->shipped_sets : ns_cfg->ignored_sets;
	bool listed = false;

	for (uint32_t i = 0; set_name != NULL && i < cf_vector_size(list); i++) {
		if (strcmp(cf_vector_get_ptr(list, i), set_name) == 0) {
			listed = true;
			break;
		}
	}

	return ns_cfg->ship_only_specified_sets ? listed : ! listed;
}


//==========================================================
// Local helpers - shipping.
//

static void*
run_ship_worker(void* udata)
{
	ship_worker* w = (ship_worker*)udata;

	while (true) {
		ship_item item;

		if (cf_queue_pop(w->q, &item, CF_QUEUE_FOREVER) != CF_QUEUE_OK) {
			continue;
		}

		ship_item_until_done(w, &item);
	}

	return NULL;
}

// Retries until the destination takes the record, or rejects it for good.
static void
ship_item_until_done(ship_worker* w, const ship_item* item)
{
	xdr_dc* dc = w->dc;
	as_namespace* ns = g_config.namespaces[item->ns_ix];
	const as_xdr_dc_ns_cfg* ns_cfg = dc->cfg->ns_cfgs[item->ns_ix];

	while (true) {
		cf_dyn_buf_define_size(db, 16 * 1024);

		// Read the current version each attempt - ships the latest state.
		build_result br = build_ship_msg(ns, ns_cfg, item, &db);

		if (br != BUILD_OK) {
			if (br == BUILD_NOT_FOUND) {
				as_incr_uint64(&dc->stats.not_found);
			}

			cf_dyn_buf_free(&db);
			return;
		}

		uint64_t start_us = cf_getus();
		uint8_t result_code;
		int rv = send_and_receive(w, db.buf, db.used_sz, &result_code);

		cf_dyn_buf_free(&db);

		if (rv != 0) {
			as_incr_uint64(&dc->stats.retry_conn);
			usleep(RETRY_SLEEP_MS * 1000);
			continue;
		}

		switch (result_code) {
		case AS_OK:
		case AS_ERR_NOT_FOUND: // delete of a record the destination lacks
		case AS_ERR_LOST_CONFLICT: // destination has a newer version
			as_incr_uint64(&dc->stats.success);
			as_add_uint64(&dc->stats.latency_us_total,
					(int64_t)(cf_getus() - start_us));
			as_store_uint64(&dc->stats.lag_ms,
					cf_clepoch_milliseconds() - item->lut);
			return;
		case AS_ERR_TIMEOUT:
		case AS_ERR_UNAVAILABLE:
		case AS_ERR_CLUSTER_KEY_MISMATCH:
		case AS_ERR_DEVICE_OVERLOAD:
		case AS_ERR_KEY_BUSY:
		case AS_ERR_XDR_KEY_BUSY:
		case AS_ERR_OUT_OF_SPACE:
		case AS_ERR_UNKNOWN:
			as_incr_uint64(&dc->stats.retry_dest);
			usleep(RETRY_SLEEP_MS * 1000);
			continue;
		case AS_SEC_ERR_NOT_AUTHENTICATED:
			disconnect(w); // re-login on the next attempt
			as_incr_uint64(&dc->stats.retry_dest);
			usleep(RETRY_SLEEP_MS * 1000);
			continue;
		default:
			as_incr_uint64(&dc->stats.abandoned);
			cf_ticker_warning(AS_XDR, "DC %s: destination rejected %pD with result %u",
					dc->cfg->name, &item->keyd, result_code);
			return;
		}
	}
}

// Builds an XDR write of the record's current version - all bins, replacing
// the destination's record - or a durable delete for a tombstone, or a
// non-durable delete for a dropped record.
static build_result
build_ship_msg(const as_namespace* ns, const as_xdr_dc_ns_cfg* ns_cfg,
		const ship_item* item, cf_dyn_buf* db)
{
	const cf_digest* keyd = &item->keyd;
	as_partition_reservation rsv;

	as_partition_reserve((as_namespace*)ns, as_partition_getid(keyd), &rsv);

	as_index_ref r_ref;

	if (as_record_get(rsv.tree, keyd, &r_ref) != 0) {
		as_partition_release(&rsv);

		if (item->is_drop) {
			build_drop_msg(ns, ns_cfg, keyd, db);
			return BUILD_OK;
		}

		return BUILD_NOT_FOUND;
	}

	as_record* r = r_ref.r;
	bool is_tombstone = ! as_record_is_live(r);
	uint32_t now_s = as_record_void_time_get();

	if (! is_tombstone && r->void_time != 0 && r->void_time <= now_s) {
		as_record_done(&r_ref, (as_namespace*)ns);
		as_partition_release(&rsv);
		return BUILD_SKIP; // expired - expirations don't ship
	}

	as_storage_rd rd;

	as_storage_record_open((as_namespace*)ns, r, &rd);

	as_bin stack_bins[RECORD_MAX_BINS];
	uint16_t n_bins = 0;

	if (! is_tombstone) {
		if (as_storage_rd_load_bins(&rd, stack_bins) < 0) {
			cf_warning(AS_XDR, "{%s} failed to load bins of %pD", ns->name,
					keyd);
			as_storage_record_close(&rd);
			as_record_done(&r_ref, (as_namespace*)ns);
			as_partition_release(&rsv);
			return BUILD_SKIP;
		}

		n_bins = rd.n_bins;
	}

	bool has_key = r->key_stored == 1 && as_storage_rd_load_key(&rd) &&
			rd.key != NULL;
	const char* set_name = as_index_get_set_name(r, (as_namespace*)ns);
	const char* ns_name = ns_cfg->remote_namespace != NULL ?
			ns_cfg->remote_namespace : ns->name;

	// Proto header - size filled in at the end.
	cf_dyn_buf_reserve(db, sizeof(as_proto), NULL);

	uint16_t n_fields = 3 + (set_name != NULL ? 1 : 0) + (has_key ? 1 : 0);
	uint16_t n_ops = 0;

	for (uint16_t i = 0; i < n_bins; i++) {
		if (as_bin_is_live(&rd.bins[i])) {
			n_ops++;
		}
	}

	as_msg m = {
			.header_sz = sizeof(as_msg),
			.info1 = AS_MSG_INFO1_XDR,
			.info2 = AS_MSG_INFO2_WRITE,
			.transaction_ttl = TRANSACTION_TTL_MS,
			.n_fields = n_fields,
			.n_ops = n_ops
	};

	if (is_tombstone) {
		m.info2 |= AS_MSG_INFO2_DELETE | AS_MSG_INFO2_DURABLE_DELETE;
	}
	else {
		m.info3 = AS_MSG_INFO3_CREATE_OR_REPLACE;
		m.record_ttl = r->void_time == 0 ? TTL_NEVER_EXPIRE :
				r->void_time - now_s;
	}

	as_msg_swap_header(&m);
	cf_dyn_buf_append_buf(db, (uint8_t*)&m, sizeof(m));

	append_field(db, AS_MSG_FIELD_TYPE_NAMESPACE, ns_name,
			(uint32_t)strlen(ns_name));

	if (set_name != NULL) {
		append_field(db, AS_MSG_FIELD_TYPE_SET, set_name,
				(uint32_t)strlen(set_name));
	}

	append_field(db, AS_MSG_FIELD_TYPE_DIGEST_RIPE, keyd, sizeof(cf_digest));

	if (has_key) {
		append_field(db, AS_MSG_FIELD_TYPE_KEY, rd.key, rd.key_size);
	}

	uint64_t lut_be = cf_swap_to_be64(r->last_update_time);

	append_field(db, AS_MSG_FIELD_TYPE_LUT, &lut_be, sizeof(lut_be));

	for (uint16_t i = 0; i < n_bins; i++) {
		as_bin* b = &rd.bins[i];

		if (! as_bin_is_live(b)) {
			continue;
		}

		size_t name_sz = strlen(b->name);
		uint32_t value_sz = as_bin_particle_client_value_size(b);
		uint8_t* at;

		cf_dyn_buf_reserve(db, sizeof(as_msg_op) + name_sz + value_sz, &at);

		as_msg_op* op = (as_msg_op*)at;

		*op = (as_msg_op){
				.op_sz = (uint32_t)(OP_FIXED_SZ + name_sz),
				.op = AS_MSG_OP_WRITE,
				.name_sz = (uint8_t)name_sz
		};

		memcpy(op->name, b->name, name_sz);
		as_bin_particle_to_client(b, op);
		op->op_sz = cf_swap_to_be32(op->op_sz);
	}

	as_storage_record_close(&rd);
	as_record_done(&r_ref, (as_namespace*)ns);
	as_partition_release(&rsv);

	finish_proto(db);

	return BUILD_OK;
}

// The record is gone, so there's no set name or key - the destination finds
// the record by digest.
static void
build_drop_msg(const as_namespace* ns, const as_xdr_dc_ns_cfg* ns_cfg,
		const cf_digest* keyd, cf_dyn_buf* db)
{
	const char* ns_name = ns_cfg->remote_namespace != NULL ?
			ns_cfg->remote_namespace : ns->name;

	cf_dyn_buf_reserve(db, sizeof(as_proto), NULL);

	as_msg m = {
			.header_sz = sizeof(as_msg),
			.info1 = AS_MSG_INFO1_XDR,
			.info2 = AS_MSG_INFO2_WRITE | AS_MSG_INFO2_DELETE,
			.transaction_ttl = TRANSACTION_TTL_MS,
			.n_fields = 2
	};

	as_msg_swap_header(&m);
	cf_dyn_buf_append_buf(db, (uint8_t*)&m, sizeof(m));

	append_field(db, AS_MSG_FIELD_TYPE_NAMESPACE, ns_name,
			(uint32_t)strlen(ns_name));
	append_field(db, AS_MSG_FIELD_TYPE_DIGEST_RIPE, keyd, sizeof(cf_digest));

	finish_proto(db);
}

static void
finish_proto(cf_dyn_buf* db)
{
	as_proto* proto = (as_proto*)db->buf;

	proto->version = PROTO_VERSION;
	proto->type = PROTO_TYPE_AS_MSG;
	proto->sz = db->used_sz - sizeof(as_proto);
	as_proto_swap(proto);
}

// Returns 0 with the destination's result code, or -1 on a connection error.
static int
send_and_receive(ship_worker* w, const uint8_t* buf, size_t sz,
		uint8_t* result_code)
{
	if (! ensure_connected(w)) {
		return -1;
	}

	uint8_t* body;
	size_t body_sz;

	if (! send_all(w->fd, buf, sz) ||
			! recv_proto(w->fd, PROTO_TYPE_AS_MSG, &body, &body_sz)) {
		disconnect(w);
		return -1;
	}

	if (body_sz < sizeof(as_msg)) {
		cf_free(body);
		disconnect(w);
		return -1;
	}

	*result_code = ((as_msg*)body)->result_code;
	cf_free(body);

	return 0;
}

// Destination writes go to any node - it proxies to the record's master.
// TODO - P3c: use the destination's partition map.
static bool
ensure_connected(ship_worker* w)
{
	if (w->fd >= 0) {
		return true;
	}

	const cf_vector* seeds = &w->dc->cfg->seed_nodes;
	uint32_t n_seeds = cf_vector_size(seeds);

	for (uint32_t i = 0; i < n_seeds; i++) {
		const dc_seed* seed = cf_vector_getp((cf_vector*)seeds,
				w->seed_ix++ % n_seeds);
		int fd = connect_seed(seed);

		if (fd < 0) {
			continue;
		}

		if (w->dc->credential != NULL && ! login(fd, w->dc)) {
			cf_ticker_warning(AS_XDR, "DC %s: login to %s:%s failed",
					w->dc->cfg->name, seed->host, seed->port);
			close(fd);
			continue;
		}

		w->fd = fd;
		return true;
	}

	cf_ticker_warning(AS_XDR, "DC %s: no destination node reachable",
			w->dc->cfg->name);

	return false;
}

static void
disconnect(ship_worker* w)
{
	if (w->fd >= 0) {
		close(w->fd);
		w->fd = -1;
	}
}

static int
connect_seed(const dc_seed* seed)
{
	struct addrinfo hints = {
			.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM
	};
	struct addrinfo* res;

	if (getaddrinfo(seed->host, seed->port, &hints, &res) != 0) {
		return -1;
	}

	int fd = -1;

	for (struct addrinfo* ai = res; ai != NULL; ai = ai->ai_next) {
		fd = socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC,
				ai->ai_protocol);

		if (fd < 0) {
			continue;
		}

		struct timeval tv = {
				.tv_sec = SOCKET_TIMEOUT_MS / 1000,
				.tv_usec = (SOCKET_TIMEOUT_MS % 1000) * 1000
		};
		int one = 1;

		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

		if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
			break;
		}

		close(fd);
		fd = -1;
	}

	freeaddrinfo(res);

	return fd;
}

// Logs in with the security protocol - authenticates this connection.
static bool
login(int fd, const xdr_dc* dc)
{
	cf_dyn_buf_define(db);
	const char* user = dc->cfg->auth_user;

	cf_dyn_buf_reserve(&db, sizeof(as_proto), NULL);

	as_sec_msg sm = {
			.scheme = AS_SEC_MSG_SCHEME, .command = SEC_CMD_LOGIN, .n_fields = 2
	};

	cf_dyn_buf_append_buf(&db, (uint8_t*)&sm, sizeof(sm));

	const char* values[2] = { user, dc->credential };
	uint8_t ids[2] = { SEC_FIELD_USER, SEC_FIELD_CREDENTIAL };

	for (int i = 0; i < 2; i++) {
		uint32_t len_be = htonl((uint32_t)strlen(values[i]) + 1);

		cf_dyn_buf_append_buf(&db, (uint8_t*)&len_be, sizeof(len_be));
		cf_dyn_buf_append_char(&db, (char)ids[i]);
		cf_dyn_buf_append_string(&db, values[i]);
	}

	as_proto* proto = (as_proto*)db.buf;

	proto->version = PROTO_VERSION;
	proto->type = PROTO_TYPE_SECURITY;
	proto->sz = db.used_sz - sizeof(as_proto);
	as_proto_swap(proto);

	uint8_t* body = NULL;
	size_t body_sz = 0;
	bool ok = send_all(fd, db.buf, db.used_sz) &&
			recv_proto(fd, PROTO_TYPE_SECURITY, &body, &body_sz) &&
			body_sz >= sizeof(as_sec_msg);

	cf_dyn_buf_free(&db);

	if (ok) {
		uint8_t result = ((as_sec_msg*)body)->result;

		ok = result == AS_OK || result == AS_SEC_ERR_NOT_CONFIGURED;
	}

	cf_free(body);

	return ok;
}

static bool
send_all(int fd, const uint8_t* buf, size_t sz)
{
	while (sz != 0) {
		ssize_t n = send(fd, buf, sz, MSG_NOSIGNAL);

		if (n <= 0) {
			if (n < 0 && errno == EINTR) {
				continue;
			}

			return false;
		}

		buf += n;
		sz -= (size_t)n;
	}

	return true;
}

static bool
recv_all(int fd, uint8_t* buf, size_t sz)
{
	while (sz != 0) {
		ssize_t n = recv(fd, buf, sz, 0);

		if (n <= 0) {
			if (n < 0 && errno == EINTR) {
				continue;
			}

			return false;
		}

		buf += n;
		sz -= (size_t)n;
	}

	return true;
}

// Reads one proto message - caller frees *body (NULL on failure).
static bool
recv_proto(int fd, uint8_t type, uint8_t** body, size_t* sz)
{
	as_proto proto;

	*body = NULL;

	if (! recv_all(fd, (uint8_t*)&proto, sizeof(proto))) {
		return false;
	}

	as_proto_swap(&proto);

	if (proto.type != type || proto.sz > PROTO_SIZE_MAX) {
		return false;
	}

	*sz = proto.sz;
	*body = cf_malloc(proto.sz == 0 ? 1 : proto.sz);

	if (! recv_all(fd, *body, proto.sz)) {
		cf_free(*body);
		*body = NULL;
		return false;
	}

	return true;
}

static void
append_field(cf_dyn_buf* db, uint8_t type, const void* data, uint32_t sz)
{
	uint32_t field_sz_be = cf_swap_to_be32(sz + 1);

	cf_dyn_buf_append_buf(db, (uint8_t*)&field_sz_be, sizeof(field_sz_be));
	cf_dyn_buf_append_char(db, (char)type);
	cf_dyn_buf_append_buf(db, (const uint8_t*)data, sz);
}

static xdr_dc*
find_dc(const char* name)
{
	for (uint32_t i = 0; i < g_n_dcs; i++) {
		if (strcmp(g_dcs[i].cfg->name, name) == 0) {
			return &g_dcs[i];
		}
	}

	return NULL;
}
