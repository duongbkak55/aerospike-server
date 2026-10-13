/*
 * security_ext.c
 *
 * Internal-user authentication and role-based access control for the ext
 * build. Replaces base/security_ce.c.
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

#include "base/security.h"

#include <arpa/inet.h>
#include <crypt.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include "aerospike/as_atomic.h"
#include "citrusleaf/alloc.h"

#include "cf_mutex.h"
#include "dynbuf.h"
#include "log.h"
#include "shash.h"
#include "socket.h"
#include "vector.h"

#include "base/batch.h"
#include "base/cfg.h"
#include "base/datamodel.h"
#include "base/proto.h"
#include "base/security_config.h"
#include "base/smd.h"
#include "base/thr_info.h"
#include "base/transaction.h"
#include "transaction/proxy.h"


//==========================================================
// Typedefs & constants.
//

// Security protocol commands.
#define CMD_AUTHENTICATE 0
#define CMD_CREATE_USER 1
#define CMD_DROP_USER 2
#define CMD_SET_PASSWORD 3
#define CMD_CHANGE_PASSWORD 4
#define CMD_GRANT_ROLES 5
#define CMD_REVOKE_ROLES 6
#define CMD_QUERY_USERS 9
#define CMD_CREATE_ROLE 10
#define CMD_DROP_ROLE 11
#define CMD_GRANT_PRIVILEGES 12
#define CMD_REVOKE_PRIVILEGES 13
#define CMD_SET_WHITELIST 14
#define CMD_SET_QUOTAS 15
#define CMD_QUERY_ROLES 16
#define CMD_LOGIN 20

// Security protocol field IDs.
#define FIELD_USER 0
#define FIELD_PASSWORD 1
#define FIELD_OLD_PASSWORD 2
#define FIELD_CREDENTIAL 3
#define FIELD_CLEAR_PASSWORD 4
#define FIELD_SESSION_TOKEN 5
#define FIELD_SESSION_TTL 6
#define FIELD_ROLES 10
#define FIELD_ROLE 11
#define FIELD_PRIVILEGES 12
#define FIELD_WHITELIST 13
#define FIELD_READ_QUOTA 14
#define FIELD_WRITE_QUOTA 15

#define N_FIELD_IDS 32

// Privilege codes on the wire. Codes below PRIV_READ are global-only.
#define PRIV_USER_ADMIN 0
#define PRIV_SYS_ADMIN 1
#define PRIV_DATA_ADMIN 2
#define PRIV_UDF_ADMIN 3
#define PRIV_SINDEX_ADMIN 4
#define PRIV_READ 10
#define PRIV_READ_WRITE 11
#define PRIV_READ_WRITE_UDF 12
#define PRIV_WRITE 13
#define PRIV_TRUNCATE 14

#define MAX_ROLE_SIZE 64 // includes null-terminator
#define MAX_ROLES_PER_USER 255
#define MAX_PRIVS_PER_ROLE 255
#define MIN_CREDENTIAL_SIZE 1
#define MAX_CREDENTIAL_SIZE 256

// Clients hash the password with this fixed bcrypt salt and send the result
// as the 'credential'.
#define CLIENT_BCRYPT_SALT "$2a$10$7EqJtq98hPqEX7fNZaFWoO"

// Stored credential hash - PBKDF2-HMAC-SHA256 of the client credential.
#define SALT_SZ 16
#define HASH_SZ 32
#define PBKDF2_ITERATIONS 10000

// Session token - version, expiry (unix seconds), HMAC keyed by the user's
// stored hash. Any node can verify it, and a password change revokes it.
#define TOKEN_VERSION 1
#define TOKEN_SZ (1 + 8 + HASH_SZ)

#define SMD_VALUE_VERSION "v1"
#define SMD_USER_PREFIX "U|"
#define SMD_ROLE_PREFIX "R|"

#define DEFAULT_ADMIN_USER "admin"
#define DEFAULT_ADMIN_PASSWORD "admin"
#define DEFAULT_ADMIN_ROLE "user-admin"
#define SMD_TIMEOUT_MS 5000

typedef struct sec_priv_s {
	uint8_t code;
	char ns[AS_ID_NAMESPACE_SZ]; // empty means all namespaces
	char set[AS_SET_NAME_MAX_SIZE]; // empty means all sets
} sec_priv;

typedef struct sec_user_s {
	char name[MAX_USER_SIZE];
	uint8_t salt[SALT_SZ];
	uint8_t hash[HASH_SZ];
	uint32_t n_roles;
	char (*roles)[MAX_ROLE_SIZE];
} sec_user;

typedef struct sec_role_s {
	char name[MAX_ROLE_SIZE];
	uint32_t n_privs;
	sec_priv* privs;
} sec_role;

// Per-connection state, hung on as_file_handle.security_filter. Batch
// sub-transactions of one connection check it concurrently, hence the lock.
typedef struct sec_conn_s {
	cf_mutex lock;
	bool authenticated;
	uint32_t gen; // g_gen when privs were resolved
	char user[MAX_USER_SIZE];
	uint8_t hash[HASH_SZ]; // user's hash at authentication
	uint32_t n_privs;
	sec_priv* privs; // effective privileges
} sec_conn;

typedef struct predefined_role_s {
	const char* name;
	uint8_t code;
} predefined_role;

static const predefined_role PREDEFINED_ROLES[] = {
		{ "user-admin", PRIV_USER_ADMIN },
		{ "sys-admin", PRIV_SYS_ADMIN },
		{ "data-admin", PRIV_DATA_ADMIN },
		{ "udf-admin", PRIV_UDF_ADMIN },
		{ "sindex-admin", PRIV_SINDEX_ADMIN },
		{ "read", PRIV_READ },
		{ "read-write", PRIV_READ_WRITE },
		{ "read-write-udf", PRIV_READ_WRITE_UDF },
		{ "write", PRIV_WRITE },
		{ "truncate", PRIV_TRUNCATE }
};

#define N_PREDEFINED_ROLES \
	(sizeof(PREDEFINED_ROLES) / sizeof(predefined_role))

// Parsed security request.
typedef struct sec_field_s {
	const uint8_t* data;
	uint32_t sz;
	bool present;
} sec_field;

typedef struct sec_request_s {
	uint8_t command;
	sec_field fields[N_FIELD_IDS];
} sec_request;


//==========================================================
// Globals.
//

static bool g_enabled = false;

static pthread_rwlock_t g_lock = PTHREAD_RWLOCK_INITIALIZER;
static cf_shash* g_users; // name -> sec_user*
static cf_shash* g_roles; // name -> sec_role* (user-defined roles only)
static uint32_t g_gen = 1; // bumped on every user or role change


//==========================================================
// Forward declarations.
//

// Startup.
static void validate_config(void);
static void default_admin_item(as_smd_item* item);
static bool read_default_password(char* password, size_t sz);

// SMD.
static void smd_accept_cb(const cf_vector* items, as_smd_accept_type type);
static void accept_user(const char* name, const char* value);
static void accept_role(const char* name, const char* value);
static char* user_smd_value(const sec_user* user);
static char* role_smd_value(const sec_role* role);
static bool smd_set_user(const sec_user* user);
static bool smd_set_role(const sec_role* role);
static bool smd_delete(const char* prefix, const char* name);

// Users & roles.
static sec_user* user_dup(const sec_user* user);
static void user_free(sec_user* user);
static sec_role* role_dup(const sec_role* role);
static void role_free(sec_role* role);
static sec_user* find_user(const char* name);
static sec_role* find_role(const char* name);
static const predefined_role* find_predefined_role(const char* name);
static bool role_exists(const char* name);
static void resolve_privs(const sec_user* user, uint32_t* n_privs,
		sec_priv** privs);

// Credentials & tokens.
static bool bcrypt_credential(const char* password, char* credential,
		size_t sz);
static void hash_credential(const uint8_t* cred, uint32_t cred_sz,
		const uint8_t* salt, uint8_t* hash);
static bool verify_credential(const sec_user* user, const uint8_t* cred,
		uint32_t cred_sz);
static void make_token(const sec_user* user, uint64_t expiry, uint8_t* token);
static uint8_t verify_token(const sec_user* user, const uint8_t* token,
		uint32_t token_sz);

// Permission checks.
static uint64_t priv_perms(uint8_t code);
static bool conn_refresh(sec_conn* conn);
static uint8_t conn_check(sec_conn* conn, const char* ns, const char* set,
		uint64_t perm);
static bool scope_matches(const sec_priv* priv, const char* ns,
		const char* set);
static int collect_reduce_cb(const void* key, void* value, void* udata);
static sec_conn* tr_conn(const as_transaction* tr);

// Requests.
static uint8_t parse_request(const uint8_t* buf, uint32_t sz,
		sec_request* req);
static bool field_string(const sec_request* req, uint8_t id, char* str,
		size_t sz);
static bool valid_name(const char* name);
static uint8_t parse_roles(const sec_field* f, uint32_t* n_roles,
		char (**roles)[MAX_ROLE_SIZE]);
static uint8_t parse_privs(const sec_field* f, uint32_t* n_privs,
		sec_priv** privs);
static uint8_t handle_request(as_file_handle* fd_h, sec_conn* conn,
		const sec_request* req, cf_dyn_buf* db);
static uint8_t cmd_login(sec_conn* conn, const sec_request* req,
		cf_dyn_buf* db);
static uint8_t cmd_authenticate(sec_conn* conn, const sec_request* req);
static uint8_t cmd_create_user(const sec_request* req);
static uint8_t cmd_drop_user(const sec_request* req);
static uint8_t cmd_set_password(const sec_request* req, bool check_old);
static uint8_t cmd_modify_roles(const sec_request* req, bool grant);
static uint8_t cmd_query_users(const sec_request* req, cf_dyn_buf* db);
static uint8_t cmd_create_role(const sec_request* req);
static uint8_t cmd_drop_role(const sec_request* req);
static uint8_t cmd_modify_privs(const sec_request* req, bool grant);
static uint8_t cmd_query_roles(const sec_request* req, cf_dyn_buf* db);

// Responses.
static void append_msg_header(cf_dyn_buf* db, uint8_t result,
		uint8_t n_fields);
static void append_field(cf_dyn_buf* db, uint8_t id, const void* data,
		uint32_t sz);
static void append_user(cf_dyn_buf* db, const sec_user* user);
static void append_role(cf_dyn_buf* db, const char* name, uint32_t n_privs,
		const sec_priv* privs);
static void send_response(as_transaction* tr, cf_dyn_buf* db);

// Logging.
static const char* conn_user(const sec_conn* conn);
static const char* command_name(uint8_t command);


//==========================================================
// Inlines & macros.
//

static inline sec_conn*
fd_conn(const as_file_handle* fd_h)
{
	return fd_h == NULL ? NULL : (sec_conn*)fd_h->security_filter;
}

static inline void
hex_encode(const uint8_t* in, uint32_t sz, char* out)
{
	static const char HEX[] = "0123456789abcdef";

	for (uint32_t i = 0; i < sz; i++) {
		out[2 * i] = HEX[in[i] >> 4];
		out[2 * i + 1] = HEX[in[i] & 0xf];
	}

	out[2 * sz] = '\0';
}

static inline bool
hex_decode(const char* in, uint32_t sz, uint8_t* out)
{
	for (uint32_t i = 0; i < sz; i++) {
		uint32_t v;

		if (sscanf(in + 2 * i, "%2x", &v) != 1) {
			return false;
		}

		out[i] = (uint8_t)v;
	}

	return true;
}


//==========================================================
// Public API.
//

void
as_security_init(void)
{
	g_enabled = g_config.sec_cfg.security_configured;

	if (! g_enabled) {
		return;
	}

	validate_config();

	g_users = cf_shash_create(cf_shash_fn_zstr, MAX_USER_SIZE,
			sizeof(sec_user*), 64, false);
	g_roles = cf_shash_create(cf_shash_fn_zstr, MAX_ROLE_SIZE,
			sizeof(sec_role*), 64, false);

	// SMD item vectors hold pointers.
	cf_vector defaults;
	as_smd_item admin;

	cf_vector_init(&defaults, sizeof(as_smd_item*), 1, 0);
	default_admin_item(&admin);
	cf_vector_append_ptr(&defaults, &admin);

	as_smd_module_load(AS_SMD_MODULE_SECURITY, smd_accept_cb, NULL, &defaults);

	cf_free(admin.key);
	cf_free(admin.value);
	cf_vector_destroy(&defaults);

	cf_info(AS_SECURITY, "security enabled - internal users");
}

void
as_security_start(void)
{
}

uint8_t
as_security_check_auth(const as_file_handle* fd_h)
{
	if (! g_enabled) {
		return AS_OK;
	}

	sec_conn* conn = fd_conn(fd_h);

	if (conn == NULL) {
		return AS_SEC_ERR_NOT_AUTHENTICATED;
	}

	cf_mutex_lock(&conn->lock);

	bool ok = conn_refresh(conn);

	cf_mutex_unlock(&conn->lock);

	return ok ? AS_OK : AS_SEC_ERR_NOT_AUTHENTICATED;
}

uint8_t
as_security_check_info_cmd(const as_file_handle* fd_h, const char* cmd,
		const char* params, as_sec_perm perm)
{
	if (! g_enabled || perm == PERM_NONE) {
		return AS_OK;
	}

	sec_conn* conn = fd_conn(fd_h);

	if (conn == NULL) {
		return AS_SEC_ERR_NOT_AUTHENTICATED;
	}

	// Scope namespace and set parameters, if any, so a namespace-scoped
	// privilege (e.g. truncate) can apply.
	char ns[AS_ID_NAMESPACE_SZ] = { 0 };
	char set[AS_SET_NAME_MAX_SIZE] = { 0 };

	if (params != NULL) {
		int ns_len = (int)sizeof(ns);

		if (as_info_param_get_namespace(params, ns, &ns_len) != INFO_PARAM_OK) {
			ns[0] = '\0';
		}

		int set_len = (int)sizeof(set);

		if (as_info_parameter_get(params, "set", set, &set_len) !=
				INFO_PARAM_OK) {
			set[0] = '\0';
		}
	}

	return conn_check(conn, ns[0] != '\0' ? ns : NULL,
			set[0] != '\0' ? set : NULL, (uint64_t)perm);
}

bool
as_security_check_data_op(as_transaction* tr, as_namespace* ns,
		as_sec_perm perm)
{
	if (! g_enabled) {
		return true;
	}

	if (tr->origin != FROM_CLIENT && tr->origin != FROM_BATCH) {
		return true; // internal, or already checked on the proxying node
	}

	sec_conn* conn = tr_conn(tr);

	if (conn == NULL) {
		tr->result_code = AS_SEC_ERR_NOT_AUTHENTICATED;
		return false;
	}

	uint64_t need = (uint64_t)perm;

	// Writes are refined to the kind of write.
	if ((need & PERM_WRITE) != 0 && as_transaction_is_delete(tr)) {
		need = (need & ~(uint64_t)PERM_WRITE) | PERM_DELETE;
	}
	else if ((need & PERM_WRITE) != 0 && as_transaction_is_udf(tr) &&
			! as_transaction_is_query(tr)) {
		need = (need & ~(uint64_t)PERM_WRITE) | PERM_UDF_APPLY;
	}

	char set[AS_SET_NAME_MAX_SIZE] = { 0 };
	as_msg_field* f = as_msg_field_get(&tr->msgp->msg, AS_MSG_FIELD_TYPE_SET);

	if (f != NULL) {
		uint32_t sz = as_msg_field_get_value_sz(f);

		if (sz < sizeof(set)) {
			memcpy(set, f->data, sz);
		}
	}

	uint8_t result = conn_check(conn, ns->name, set[0] != '\0' ? set : NULL,
			need);

	if (result != AS_OK) {
		if (g_config.sec_cfg.report.violation) {
			cf_info(AS_SECURITY,
					"permitted false | user: %s | action: data op | detail: ns %s set %s perm 0x%lx | result %u",
					conn_user(conn), ns->name, set, need, result);
		}

		tr->result_code = result;
		return false;
	}

	return true;
}

int
as_security_check_rps(as_file_handle* fd_h, uint32_t rps, as_sec_perm perm,
		bool is_write, void** udata)
{
	// Quotas are not yet implemented (P1b) - see validate_config().
	*udata = NULL;
	return AS_OK;
}

uint8_t
as_security_check_permission(const as_transaction* tr, const char* username,
		uint32_t ns_ix, uint16_t set_id, as_sec_perm perm)
{
	if (! g_enabled) {
		return AS_OK;
	}

	pthread_rwlock_rdlock(&g_lock);

	sec_user* user = find_user(username);
	uint32_t n_privs = 0;
	sec_priv* privs = NULL;

	if (user != NULL) {
		resolve_privs(user, &n_privs, &privs);
	}

	pthread_rwlock_unlock(&g_lock);

	if (user == NULL) {
		return AS_SEC_ERR_USER;
	}

	as_namespace* ns = g_config.namespaces[ns_ix];
	const char* set = as_namespace_get_set_name(ns, set_id);
	uint64_t have = 0;

	for (uint32_t i = 0; i < n_privs; i++) {
		if (scope_matches(&privs[i], ns->name, set)) {
			have |= priv_perms(privs[i].code);
		}
	}

	cf_free(privs);

	return (have & (uint64_t)perm) == (uint64_t)perm ?
			AS_OK : AS_SEC_ERR_ROLE_VIOLATION;
}

bool
as_security_copy_username(const as_transaction* tr, char* dst, uint32_t dst_sz)
{
	if (! g_enabled) {
		return false;
	}

	if (tr->origin == FROM_PROXY) {
		if (tr->from.proxy_orig == NULL ||
				tr->from.proxy_orig->username[0] == '\0') {
			return false;
		}

		snprintf(dst, dst_sz, "%s", tr->from.proxy_orig->username);
		return true;
	}

	sec_conn* conn = tr_conn(tr);

	if (conn == NULL) {
		return false;
	}

	cf_mutex_lock(&conn->lock);

	bool copied = conn->authenticated;

	if (copied) {
		snprintf(dst, dst_sz, "%s", conn->user);
	}

	cf_mutex_unlock(&conn->lock);

	return copied;
}

void
as_security_done_rps(void* udata, uint32_t rps, bool is_write)
{
	// Nothing to do - no quotas yet, and udata is always NULL.
}

void*
as_security_filter_create(void)
{
	if (! g_enabled) {
		return NULL;
	}

	sec_conn* conn = cf_calloc(1, sizeof(sec_conn));

	cf_mutex_init(&conn->lock);

	return conn;
}

void
as_security_filter_destroy(void* pv_filter)
{
	sec_conn* conn = (sec_conn*)pv_filter;

	cf_free(conn->privs);
	cf_free(conn);
}

void
as_audit_log(const as_transaction* tr, const as_file_handle* fd_h, uint8_t res,
		const char* action, const char* detail)
{
	if (! g_enabled || ! g_config.sec_cfg.report.data_op) {
		return;
	}

	cf_info(AS_SECURITY, "permitted %s | client: %s | user: %s | action: %s | detail: %s",
			res == AS_OK ? "true" : "false",
			fd_h != NULL ? fd_h->client : "internal", conn_user(fd_conn(fd_h)),
			action != NULL ? action : "", detail != NULL ? detail : "");
}

void
as_security_log(const as_file_handle* fd_h, uint8_t result, as_sec_perm perm,
		const char* action, const char* detail)
{
	if (! g_enabled) {
		return;
	}

	const as_sec_report* report = &g_config.sec_cfg.report;

	bool log = result != AS_OK ? report->violation :
			(perm & (PERM_SET_CONFIG | PERM_LOGGING_CTRL | PERM_SERVICE_CTRL |
					PERM_XDR_SET_FILTER)) != 0 && report->sys_admin;

	if (! log) {
		return;
	}

	cf_info(AS_SECURITY, "permitted %s | client: %s | user: %s | action: %s | detail: %s",
			result == AS_OK ? "true" : "false",
			fd_h != NULL ? fd_h->client : "internal", conn_user(fd_conn(fd_h)),
			action != NULL ? action : "", detail != NULL ? detail : "");
}

bool
as_security_should_refresh(void)
{
	// Connections re-resolve privileges lazily when g_gen changes.
	return false;
}

void
as_security_refresh(as_file_handle* fd_h)
{
}

void
as_security_transact(as_transaction* tr)
{
	as_file_handle* fd_h = tr->from.proto_fd_h;
	sec_conn* conn = fd_conn(fd_h);

	cf_dyn_buf_define_size(db, 1024);

	// Reserve space for the proto header, filled in by send_response().
	cf_dyn_buf_reserve(&db, sizeof(as_proto), NULL);

	sec_request req;
	uint8_t result = parse_request(tr->msgp->proto.body, tr->msgp->proto.sz,
			&req);

	if (result == AS_OK && ! g_enabled) {
		// Clients treat this as "no login needed".
		result = AS_SEC_ERR_NOT_CONFIGURED;
	}

	if (result == AS_OK) {
		result = handle_request(fd_h, conn, &req, &db);
	}

	if (result != AS_OK) {
		db.used_sz = sizeof(as_proto);
		append_msg_header(&db, result, 0);
	}

	cf_free(tr->msgp);
	tr->msgp = NULL;

	send_response(tr, &db);
	cf_dyn_buf_free(&db);
}

void
as_security_config_log_scope(const char* ns_name, const char* set_name)
{
	cf_warning(AS_SECURITY, "report-data-op scope {%s|%s} ignored - data-op reporting is global in this build",
			ns_name, set_name != NULL ? set_name : "");
	g_config.sec_cfg.report.data_op = true;
}

void
as_security_config_log_role(const char* role)
{
	cf_warning(AS_SECURITY, "report-data-op-role %s ignored - data-op reporting is global in this build",
			role);
	g_config.sec_cfg.report.data_op = true;
}

void
as_security_config_log_user(const char* user)
{
	cf_warning(AS_SECURITY, "report-data-op-user %s ignored - data-op reporting is global in this build",
			user);
	g_config.sec_cfg.report.data_op = true;
}

void
as_security_get_config(cf_dyn_buf* db)
{
	if (! g_enabled) {
		return;
	}

	const as_sec_config* cfg = &g_config.sec_cfg;

	info_append_bool(db, "enable-quotas", cfg->quotas_enabled);
	info_append_uint32(db, "privilege-refresh-period",
			cfg->privilege_refresh_period);
	info_append_uint32(db, "session-ttl", cfg->session_ttl);
	info_append_uint32(db, "tps-weight", cfg->tps_weight);
	info_append_bool(db, "log.report-authentication",
			cfg->report.authentication);
	info_append_bool(db, "log.report-data-op", cfg->report.data_op);
	info_append_bool(db, "log.report-sys-admin", cfg->report.sys_admin);
	info_append_bool(db, "log.report-user-admin", cfg->report.user_admin);
	info_append_bool(db, "log.report-violation", cfg->report.violation);
}

bool
as_security_set_config(const char* cmd)
{
	if (! g_enabled) {
		return false;
	}

	as_sec_config* cfg = &g_config.sec_cfg;
	char v[64];
	int v_len = (int)sizeof(v);

	if (as_info_parameter_get(cmd, "session-ttl", v, &v_len) == 0) {
		char* end;
		unsigned long val = strtoul(v, &end, 10);

		if (*end != '\0' || val < SECURITY_SESSION_TTL_MIN ||
				val > SECURITY_SESSION_TTL_MAX) {
			return false;
		}

		cf_info(AS_SECURITY, "changing session-ttl from %u to %lu",
				cfg->session_ttl, val);
		cfg->session_ttl = (uint32_t)val;
		return true;
	}

	static const struct {
		const char* name;
		size_t offset;
	} REPORTS[] = {
			{ "report-authentication", offsetof(as_sec_report, authentication) },
			{ "report-data-op", offsetof(as_sec_report, data_op) },
			{ "report-sys-admin", offsetof(as_sec_report, sys_admin) },
			{ "report-user-admin", offsetof(as_sec_report, user_admin) },
			{ "report-violation", offsetof(as_sec_report, violation) }
	};

	for (size_t i = 0; i < sizeof(REPORTS) / sizeof(REPORTS[0]); i++) {
		v_len = (int)sizeof(v);

		if (as_info_parameter_get(cmd, REPORTS[i].name, v, &v_len) != 0) {
			continue;
		}

		bool* field = (bool*)((uint8_t*)&cfg->report + REPORTS[i].offset);

		if (strcmp(v, "true") == 0) {
			*field = true;
		}
		else if (strcmp(v, "false") == 0) {
			*field = false;
		}
		else {
			return false;
		}

		cf_info(AS_SECURITY, "changing %s to %s", REPORTS[i].name, v);
		return true;
	}

	return false;
}


//==========================================================
// Local helpers - startup.
//

static void
validate_config(void)
{
	const as_sec_config* cfg = &g_config.sec_cfg;

	if (cfg->ldap_configured) {
		cf_crash_nostack(AS_SECURITY, "security 'ldap' is not supported - internal users only");
	}

	if (cfg->quotas_enabled) {
		cf_crash_nostack(AS_SECURITY, "security 'enable-quotas' is not yet implemented (P1b)");
	}
}

// The default admin user is an SMD default item - it has timestamp 0, so any
// real change (new password, drop) on any node wins over it.
static void
default_admin_item(as_smd_item* item)
{
	char password[MAX_CREDENTIAL_SIZE];

	if (! read_default_password(password, sizeof(password))) {
		strcpy(password, DEFAULT_ADMIN_PASSWORD);
	}

	char cred[MAX_CREDENTIAL_SIZE];

	if (! bcrypt_credential(password, cred, sizeof(cred))) {
		cf_crash_nostack(AS_SECURITY, "can't hash default admin password");
	}

	memset(password, 0, sizeof(password));

	// Fixed salt keeps the default item identical on every node.
	sec_user admin = { .name = DEFAULT_ADMIN_USER, .n_roles = 1 };
	char roles[1][MAX_ROLE_SIZE] = { DEFAULT_ADMIN_ROLE };

	memcpy(admin.salt, "default-admin-sl", SALT_SZ);
	hash_credential((const uint8_t*)cred, (uint32_t)strlen(cred), admin.salt,
			admin.hash);
	admin.roles = roles;

	char key[MAX_USER_SIZE + 2];

	snprintf(key, sizeof(key), SMD_USER_PREFIX "%s", DEFAULT_ADMIN_USER);

	*item = (as_smd_item){
			.key = cf_strdup(key),
			.value = user_smd_value(&admin)
	};
}

static bool
read_default_password(char* password, size_t sz)
{
	const char* path = g_config.sec_cfg.default_password_file;

	if (path == NULL) {
		return false;
	}

	FILE* f = fopen(path, "r");

	if (f == NULL) {
		cf_crash_nostack(AS_SECURITY, "can't open default-password-file %s: %s",
				path, cf_strerror(errno));
	}

	if (fgets(password, (int)sz, f) == NULL) {
		cf_crash_nostack(AS_SECURITY, "can't read default-password-file %s",
				path);
	}

	fclose(f);

	password[strcspn(password, "\r\n")] = '\0';

	if (password[0] == '\0') {
		cf_crash_nostack(AS_SECURITY, "empty default-password-file %s", path);
	}

	return true;
}


//==========================================================
// Local helpers - SMD.
//

static void
smd_accept_cb(const cf_vector* items, as_smd_accept_type type)
{
	pthread_rwlock_wrlock(&g_lock);

	for (uint32_t i = 0; i < cf_vector_size(items); i++) {
		const as_smd_item* item = cf_vector_get_ptr(items, i);

		if (strncmp(item->key, SMD_USER_PREFIX, 2) == 0) {
			accept_user(item->key + 2, item->value);
		}
		else if (strncmp(item->key, SMD_ROLE_PREFIX, 2) == 0) {
			accept_role(item->key + 2, item->value);
		}
		else {
			cf_warning(AS_SECURITY, "ignoring unknown smd key %s", item->key);
		}
	}

	as_incr_uint32(&g_gen);

	pthread_rwlock_unlock(&g_lock);
}

// Value: v1|<salt-hex>|<hash-hex>|<role>,<role>,...
static void
accept_user(const char* name, const char* value)
{
	sec_user* old = find_user(name);

	if (old != NULL) {
		cf_shash_delete(g_users, old->name);
		user_free(old);
	}

	if (value == NULL) {
		return; // deleted
	}

	sec_user* user = cf_calloc(1, sizeof(sec_user));
	char salt_hex[2 * SALT_SZ + 1];
	char hash_hex[2 * HASH_SZ + 1];
	int roles_at = 0;

	if (strlen(name) >= MAX_USER_SIZE ||
			sscanf(value, SMD_VALUE_VERSION "|%32[0-9a-f]|%64[0-9a-f]|%n",
					salt_hex, hash_hex, &roles_at) != 2 || roles_at == 0 ||
			! hex_decode(salt_hex, SALT_SZ, user->salt) ||
			! hex_decode(hash_hex, HASH_SZ, user->hash)) {
		cf_warning(AS_SECURITY, "ignoring bad smd user %s", name);
		cf_free(user);
		return;
	}

	strcpy(user->name, name);

	const char* roles = value + roles_at;

	if (*roles != '\0') {
		user->n_roles = 1;

		for (const char* c = roles; *c != '\0'; c++) {
			if (*c == ',') {
				user->n_roles++;
			}
		}

		user->roles = cf_calloc(user->n_roles, MAX_ROLE_SIZE);

		const char* c = roles;

		for (uint32_t i = 0; i < user->n_roles; i++) {
			size_t len = strcspn(c, ",");

			if (len >= MAX_ROLE_SIZE) {
				len = MAX_ROLE_SIZE - 1;
			}

			memcpy(user->roles[i], c, len);
			c += strcspn(c, ",");
			c += *c == ',' ? 1 : 0;
		}
	}

	cf_shash_put(g_users, user->name, &user);
}

// Value: v1|<code>:<ns>:<set>,<code>:<ns>:<set>,...
static void
accept_role(const char* name, const char* value)
{
	sec_role* old = find_role(name);

	if (old != NULL) {
		cf_shash_delete(g_roles, old->name);
		role_free(old);
	}

	if (value == NULL) {
		return; // deleted
	}

	if (strlen(name) >= MAX_ROLE_SIZE ||
			strncmp(value, SMD_VALUE_VERSION "|", 3) != 0) {
		cf_warning(AS_SECURITY, "ignoring bad smd role %s", name);
		return;
	}

	sec_role* role = cf_calloc(1, sizeof(sec_role));
	const char* privs = value + 3;

	strcpy(role->name, name);

	if (*privs != '\0') {
		uint32_t n = 1;

		for (const char* c = privs; *c != '\0'; c++) {
			if (*c == ',') {
				n++;
			}
		}

		role->privs = cf_calloc(n, sizeof(sec_priv));

		const char* c = privs;

		for (uint32_t i = 0; i < n; i++) {
			sec_priv* p = &role->privs[role->n_privs];
			unsigned code;
			int ns_at = 0;

			if (sscanf(c, "%u:%n", &code, &ns_at) == 1 && ns_at != 0) {
				const char* ns = c + ns_at;
				size_t ns_len = strcspn(ns, ":");
				const char* set = ns + ns_len + (ns[ns_len] == ':' ? 1 : 0);
				size_t set_len = strcspn(set, ",");

				if (ns_len < sizeof(p->ns) && set_len < sizeof(p->set)) {
					p->code = (uint8_t)code;
					memcpy(p->ns, ns, ns_len);
					memcpy(p->set, set, set_len);
					role->n_privs++;
				}
			}

			c += strcspn(c, ",");
			c += *c == ',' ? 1 : 0;
		}
	}

	cf_shash_put(g_roles, role->name, &role);
}

static char*
user_smd_value(const sec_user* user)
{
	cf_dyn_buf_define(db);
	char salt_hex[2 * SALT_SZ + 1];
	char hash_hex[2 * HASH_SZ + 1];

	hex_encode(user->salt, SALT_SZ, salt_hex);
	hex_encode(user->hash, HASH_SZ, hash_hex);

	// Note - cf_dyn_buf_append_format() truncates at 100 characters.
	cf_dyn_buf_append_string(&db, SMD_VALUE_VERSION "|");
	cf_dyn_buf_append_string(&db, salt_hex);
	cf_dyn_buf_append_char(&db, '|');
	cf_dyn_buf_append_string(&db, hash_hex);
	cf_dyn_buf_append_char(&db, '|');

	for (uint32_t i = 0; i < user->n_roles; i++) {
		if (i != 0) {
			cf_dyn_buf_append_char(&db, ',');
		}

		cf_dyn_buf_append_string(&db, user->roles[i]);
	}

	char* value = cf_dyn_buf_strdup(&db);

	cf_dyn_buf_free(&db);

	return value != NULL ? value : cf_strdup("");
}

static char*
role_smd_value(const sec_role* role)
{
	cf_dyn_buf_define(db);

	cf_dyn_buf_append_string(&db, SMD_VALUE_VERSION "|");

	for (uint32_t i = 0; i < role->n_privs; i++) {
		const sec_priv* p = &role->privs[i];

		if (i != 0) {
			cf_dyn_buf_append_char(&db, ',');
		}

		cf_dyn_buf_append_uint32(&db, p->code);
		cf_dyn_buf_append_char(&db, ':');
		cf_dyn_buf_append_string(&db, p->ns);
		cf_dyn_buf_append_char(&db, ':');
		cf_dyn_buf_append_string(&db, p->set);
	}

	char* value = cf_dyn_buf_strdup(&db);

	cf_dyn_buf_free(&db);

	return value;
}

static bool
smd_set_user(const sec_user* user)
{
	char key[MAX_USER_SIZE + 2];
	char* value = user_smd_value(user);

	snprintf(key, sizeof(key), SMD_USER_PREFIX "%s", user->name);

	bool ok = as_smd_set_blocking(AS_SMD_MODULE_SECURITY, key, value,
			SMD_TIMEOUT_MS);

	cf_free(value);

	return ok;
}

static bool
smd_set_role(const sec_role* role)
{
	char key[MAX_ROLE_SIZE + 2];
	char* value = role_smd_value(role);

	snprintf(key, sizeof(key), SMD_ROLE_PREFIX "%s", role->name);

	bool ok = as_smd_set_blocking(AS_SMD_MODULE_SECURITY, key, value,
			SMD_TIMEOUT_MS);

	cf_free(value);

	return ok;
}

static bool
smd_delete(const char* prefix, const char* name)
{
	char key[MAX_ROLE_SIZE + 2];

	snprintf(key, sizeof(key), "%s%s", prefix, name);

	return as_smd_delete_blocking(AS_SMD_MODULE_SECURITY, key, SMD_TIMEOUT_MS);
}


//==========================================================
// Local helpers - users & roles. Callers hold g_lock where needed.
//

static sec_user*
user_dup(const sec_user* user)
{
	sec_user* dup = cf_malloc(sizeof(sec_user));

	*dup = *user;

	if (user->n_roles != 0) {
		dup->roles = cf_malloc(user->n_roles * MAX_ROLE_SIZE);
		memcpy(dup->roles, user->roles, user->n_roles * MAX_ROLE_SIZE);
	}
	else {
		dup->roles = NULL;
	}

	return dup;
}

static void
user_free(sec_user* user)
{
	cf_free(user->roles);
	cf_free(user);
}

static sec_role*
role_dup(const sec_role* role)
{
	sec_role* dup = cf_malloc(sizeof(sec_role));

	*dup = *role;

	if (role->n_privs != 0) {
		dup->privs = cf_malloc(role->n_privs * sizeof(sec_priv));
		memcpy(dup->privs, role->privs, role->n_privs * sizeof(sec_priv));
	}
	else {
		dup->privs = NULL;
	}

	return dup;
}

static void
role_free(sec_role* role)
{
	cf_free(role->privs);
	cf_free(role);
}

static sec_user*
find_user(const char* name)
{
	char key[MAX_USER_SIZE] = { 0 };
	sec_user* user = NULL;

	if (strlen(name) >= sizeof(key)) {
		return NULL;
	}

	strcpy(key, name);
	cf_shash_get(g_users, key, &user);

	return user;
}

static sec_role*
find_role(const char* name)
{
	char key[MAX_ROLE_SIZE] = { 0 };
	sec_role* role = NULL;

	if (strlen(name) >= sizeof(key)) {
		return NULL;
	}

	strcpy(key, name);
	cf_shash_get(g_roles, key, &role);

	return role;
}

static const predefined_role*
find_predefined_role(const char* name)
{
	for (uint32_t i = 0; i < N_PREDEFINED_ROLES; i++) {
		if (strcmp(PREDEFINED_ROLES[i].name, name) == 0) {
			return &PREDEFINED_ROLES[i];
		}
	}

	return NULL;
}

static bool
role_exists(const char* name)
{
	return find_predefined_role(name) != NULL || find_role(name) != NULL;
}

// Roles that no longer exist are skipped. Caller frees *privs.
static void
resolve_privs(const sec_user* user, uint32_t* n_privs, sec_priv** privs)
{
	uint32_t n = 0;

	for (uint32_t i = 0; i < user->n_roles; i++) {
		const sec_role* role = find_role(user->roles[i]);

		n += role != NULL ? role->n_privs : 1;
	}

	sec_priv* out = cf_calloc(n == 0 ? 1 : n, sizeof(sec_priv));
	uint32_t at = 0;

	for (uint32_t i = 0; i < user->n_roles; i++) {
		const predefined_role* pre = find_predefined_role(user->roles[i]);

		if (pre != NULL) {
			out[at++].code = pre->code;
			continue;
		}

		const sec_role* role = find_role(user->roles[i]);

		if (role != NULL) {
			memcpy(&out[at], role->privs, role->n_privs * sizeof(sec_priv));
			at += role->n_privs;
		}
	}

	*n_privs = at;
	*privs = out;
}


//==========================================================
// Local helpers - credentials & tokens.
//

static bool
bcrypt_credential(const char* password, char* credential, size_t sz)
{
	struct crypt_data data;

	memset(&data, 0, sizeof(data));

	const char* hash = crypt_r(password, CLIENT_BCRYPT_SALT, &data);

	if (hash == NULL || hash[0] == '*' || strlen(hash) >= sz) {
		return false;
	}

	strcpy(credential, hash);
	memset(&data, 0, sizeof(data));

	return true;
}

static void
hash_credential(const uint8_t* cred, uint32_t cred_sz, const uint8_t* salt,
		uint8_t* hash)
{
	if (PKCS5_PBKDF2_HMAC((const char*)cred, (int)cred_sz, salt, SALT_SZ,
			PBKDF2_ITERATIONS, EVP_sha256(), HASH_SZ, hash) != 1) {
		cf_crash(AS_SECURITY, "PBKDF2 failed");
	}
}

static bool
verify_credential(const sec_user* user, const uint8_t* cred, uint32_t cred_sz)
{
	uint8_t hash[HASH_SZ];

	hash_credential(cred, cred_sz, user->salt, hash);

	return CRYPTO_memcmp(hash, user->hash, HASH_SZ) == 0;
}

static void
token_mac(const sec_user* user, const uint8_t* expiry_be, uint8_t* mac)
{
	static const char DOMAIN[] = "as-ext-session-v1";

	uint8_t data[sizeof(DOMAIN) + MAX_USER_SIZE + 8];
	size_t name_len = strlen(user->name);
	uint8_t* at = data;

	memcpy(at, DOMAIN, sizeof(DOMAIN));
	at += sizeof(DOMAIN);
	memcpy(at, user->name, name_len + 1);
	at += name_len + 1;
	memcpy(at, expiry_be, 8);
	at += 8;

	unsigned int mac_sz = HASH_SZ;

	if (HMAC(EVP_sha256(), user->hash, HASH_SZ, data, (size_t)(at - data), mac,
			&mac_sz) == NULL) {
		cf_crash(AS_SECURITY, "HMAC failed");
	}
}

static void
make_token(const sec_user* user, uint64_t expiry, uint8_t* token)
{
	token[0] = TOKEN_VERSION;

	for (int i = 0; i < 8; i++) {
		token[1 + i] = (uint8_t)(expiry >> (56 - 8 * i));
	}

	token_mac(user, token + 1, token + 9);
}

static uint8_t
verify_token(const sec_user* user, const uint8_t* token, uint32_t token_sz)
{
	if (token_sz != TOKEN_SZ || token[0] != TOKEN_VERSION) {
		return AS_SEC_ERR_CREDENTIAL;
	}

	uint8_t mac[HASH_SZ];

	token_mac(user, token + 1, mac);

	if (CRYPTO_memcmp(mac, token + 9, HASH_SZ) != 0) {
		return AS_SEC_ERR_CREDENTIAL;
	}

	uint64_t expiry = 0;

	for (int i = 0; i < 8; i++) {
		expiry = (expiry << 8) | token[1 + i];
	}

	return (uint64_t)time(NULL) < expiry ? AS_OK : AS_SEC_ERR_EXPIRED_SESSION;
}


//==========================================================
// Local helpers - permission checks.
//

static uint64_t
priv_perms(uint8_t code)
{
	switch (code) {
	case PRIV_USER_ADMIN:
		return PERM_USER_ADMIN;
	case PRIV_SYS_ADMIN:
		return PERM_SET_CONFIG | PERM_LOGGING_CTRL | PERM_SERVICE_CTRL |
				PERM_XDR_SET_FILTER | PERM_QUERY_ADMIN | PERM_EVICT_ADMIN |
				PERM_RECORD_INFO;
	case PRIV_DATA_ADMIN:
		return PERM_UDF_ADMIN | PERM_SINDEX_ADMIN | PERM_TRUNCATE |
				PERM_QUERY_ADMIN | PERM_EVICT_ADMIN;
	case PRIV_UDF_ADMIN:
		return PERM_UDF_ADMIN;
	case PRIV_SINDEX_ADMIN:
		return PERM_SINDEX_ADMIN;
	case PRIV_READ:
		return PERM_READ | PERM_QUERY;
	case PRIV_READ_WRITE:
		return PERM_READ | PERM_QUERY | PERM_WRITE | PERM_DELETE |
				PERM_OPS_QUERY;
	case PRIV_READ_WRITE_UDF:
		return PERM_READ | PERM_QUERY | PERM_WRITE | PERM_DELETE |
				PERM_OPS_QUERY | PERM_UDF_APPLY | PERM_UDF_QUERY;
	case PRIV_WRITE:
		return PERM_WRITE | PERM_DELETE;
	case PRIV_TRUNCATE:
		return PERM_TRUNCATE;
	default:
		return 0;
	}
}

static bool
valid_priv_code(uint8_t code)
{
	return code <= PRIV_SINDEX_ADMIN ||
			(code >= PRIV_READ && code <= PRIV_TRUNCATE);
}

// Caller holds conn->lock. Re-resolves privileges if users or roles changed,
// and drops authentication if the user was dropped or its password changed.
static bool
conn_refresh(sec_conn* conn)
{
	if (! conn->authenticated) {
		return false;
	}

	uint32_t gen = as_load_uint32(&g_gen);

	if (conn->gen == gen) {
		return true;
	}

	pthread_rwlock_rdlock(&g_lock);

	const sec_user* user = find_user(conn->user);

	cf_free(conn->privs);
	conn->privs = NULL;
	conn->n_privs = 0;

	if (user == NULL || CRYPTO_memcmp(user->hash, conn->hash, HASH_SZ) != 0) {
		conn->authenticated = false;
	}
	else {
		resolve_privs(user, &conn->n_privs, &conn->privs);
		conn->gen = gen;
	}

	pthread_rwlock_unlock(&g_lock);

	return conn->authenticated;
}

static uint8_t
conn_check(sec_conn* conn, const char* ns, const char* set, uint64_t perm)
{
	cf_mutex_lock(&conn->lock);

	if (! conn_refresh(conn)) {
		cf_mutex_unlock(&conn->lock);
		return AS_SEC_ERR_NOT_AUTHENTICATED;
	}

	uint64_t have = 0;

	for (uint32_t i = 0; i < conn->n_privs; i++) {
		if (scope_matches(&conn->privs[i], ns, set)) {
			have |= priv_perms(conn->privs[i].code);
		}
	}

	cf_mutex_unlock(&conn->lock);

	return (have & perm) == perm ? AS_OK : AS_SEC_ERR_ROLE_VIOLATION;
}

// A privilege applies if it is global, or scoped to the target namespace (and
// set, if it has one). With no target namespace, only global ones apply.
static bool
scope_matches(const sec_priv* priv, const char* ns, const char* set)
{
	if (priv->ns[0] == '\0') {
		return true;
	}

	if (ns == NULL || strcmp(priv->ns, ns) != 0) {
		return false;
	}

	return priv->set[0] == '\0' || (set != NULL && strcmp(priv->set, set) == 0);
}

// Collects hash values (pointers) into a cf_vector.
static int
collect_reduce_cb(const void* key, void* value, void* udata)
{
	cf_vector_append((cf_vector*)udata, value);
	return CF_SHASH_OK;
}

static sec_conn*
tr_conn(const as_transaction* tr)
{
	switch (tr->origin) {
	case FROM_CLIENT:
		return fd_conn(tr->from.proto_fd_h);
	case FROM_BATCH:
		return fd_conn(as_batch_get_fd_h(tr->from.batch_shared));
	default:
		return NULL;
	}
}


//==========================================================
// Local helpers - requests.
//

static uint8_t
parse_request(const uint8_t* buf, uint32_t sz, sec_request* req)
{
	memset(req, 0, sizeof(sec_request));

	if (sz < sizeof(as_sec_msg)) {
		return AS_SEC_ERR_FIELD;
	}

	const as_sec_msg* m = (const as_sec_msg*)buf;

	if (m->scheme != AS_SEC_MSG_SCHEME) {
		return AS_SEC_ERR_SCHEME;
	}

	req->command = m->command;

	const uint8_t* at = buf + sizeof(as_sec_msg);
	const uint8_t* end = buf + sz;

	for (uint8_t i = 0; i < m->n_fields; i++) {
		if (end - at < 5) {
			return AS_SEC_ERR_FIELD;
		}

		uint32_t len = ntohl(*(const uint32_t*)at);
		uint8_t id = at[4];

		if (len == 0 || len - 1 > (uint32_t)(end - at - 5)) {
			return AS_SEC_ERR_FIELD;
		}

		if (id < N_FIELD_IDS) {
			req->fields[id] = (sec_field){
				.data = at + 5, .sz = len - 1, .present = true
			};
		}

		at += 4 + len;
	}

	return AS_OK;
}

static bool
field_string(const sec_request* req, uint8_t id, char* str, size_t sz)
{
	const sec_field* f = &req->fields[id];

	if (! f->present || f->sz == 0 || f->sz >= sz ||
			memchr(f->data, '\0', f->sz) != NULL) {
		return false;
	}

	memcpy(str, f->data, f->sz);
	str[f->sz] = '\0';

	return true;
}

// Names go into SMD keys and values - keep the delimiters out.
static bool
valid_name(const char* name)
{
	for (const char* c = name; *c != '\0'; c++) {
		if (*c < 0x20 || *c == 0x7f || *c == '|' || *c == ',' || *c == ':') {
			return false;
		}
	}

	return name[0] != '\0';
}

static uint8_t
parse_roles(const sec_field* f, uint32_t* n_roles,
		char (**roles)[MAX_ROLE_SIZE])
{
	if (! f->present || f->sz == 0) {
		return AS_SEC_ERR_ROLE;
	}

	const uint8_t* at = f->data;
	const uint8_t* end = f->data + f->sz;
	uint32_t n = *at++;

	char (*out)[MAX_ROLE_SIZE] = cf_calloc(n == 0 ? 1 : n, MAX_ROLE_SIZE);

	for (uint32_t i = 0; i < n; i++) {
		if (at >= end || *at == 0 || *at >= MAX_ROLE_SIZE ||
				*at > end - at - 1) {
			cf_free(out);
			return AS_SEC_ERR_ROLE;
		}

		memcpy(out[i], at + 1, *at);
		at += 1 + *at;

		if (! valid_name(out[i])) {
			cf_free(out);
			return AS_SEC_ERR_ROLE;
		}
	}

	*n_roles = n;
	*roles = out;

	return AS_OK;
}

static uint8_t
parse_privs(const sec_field* f, uint32_t* n_privs, sec_priv** privs)
{
	if (! f->present || f->sz == 0) {
		return AS_SEC_ERR_PRIVILEGE;
	}

	const uint8_t* at = f->data;
	const uint8_t* end = f->data + f->sz;
	uint32_t n = *at++;
	sec_priv* out = cf_calloc(n == 0 ? 1 : n, sizeof(sec_priv));

	for (uint32_t i = 0; i < n; i++) {
		if (at >= end || ! valid_priv_code(*at)) {
			cf_free(out);
			return AS_SEC_ERR_PRIVILEGE;
		}

		out[i].code = *at++;

		if (out[i].code < PRIV_READ) {
			continue; // global-only
		}

		// Namespace, then set - each a length byte and the string.
		char* dst[2] = { out[i].ns, out[i].set };
		size_t dst_sz[2] = { sizeof(out[i].ns), sizeof(out[i].set) };

		for (int s = 0; s < 2; s++) {
			if (at >= end || *at >= dst_sz[s] || *at > end - at - 1) {
				cf_free(out);
				return AS_SEC_ERR_PRIVILEGE;
			}

			memcpy(dst[s], at + 1, *at);
			at += 1 + *at;

			if (dst[s][0] != '\0' && ! valid_name(dst[s])) {
				cf_free(out);
				return AS_SEC_ERR_PRIVILEGE;
			}
		}

		if (out[i].ns[0] == '\0' && out[i].set[0] != '\0') {
			cf_free(out);
			return AS_SEC_ERR_PRIVILEGE;
		}
	}

	*n_privs = n;
	*privs = out;

	return AS_OK;
}

static uint8_t
handle_request(as_file_handle* fd_h, sec_conn* conn, const sec_request* req,
		cf_dyn_buf* db)
{
	if (conn == NULL) {
		return AS_SEC_ERR_STATE;
	}

	uint8_t command = req->command;

	if (command == CMD_LOGIN) {
		return cmd_login(conn, req, db);
	}

	if (command == CMD_AUTHENTICATE) {
		uint8_t result = cmd_authenticate(conn, req);

		if (result == AS_OK) {
			append_msg_header(db, AS_OK, 0);
		}

		return result;
	}

	// Everything else needs an authenticated connection.
	if (as_security_check_auth(fd_h) != AS_OK) {
		return AS_SEC_ERR_NOT_AUTHENTICATED;
	}

	char self[MAX_USER_SIZE];
	char target[MAX_USER_SIZE];

	cf_mutex_lock(&conn->lock);
	strcpy(self, conn->user);
	cf_mutex_unlock(&conn->lock);

	bool is_self = field_string(req, FIELD_USER, target, sizeof(target)) &&
			strcmp(target, self) == 0;

	// A user may change its own password (with the old one) and query itself.
	bool self_ok = is_self && (command == CMD_CHANGE_PASSWORD ||
			command == CMD_QUERY_USERS);

	if (! self_ok && conn_check(conn, NULL, NULL, PERM_USER_ADMIN) != AS_OK) {
		as_security_log(fd_h, AS_SEC_ERR_ROLE_VIOLATION, PERM_USER_ADMIN,
				command_name(command), target);
		return AS_SEC_ERR_ROLE_VIOLATION;
	}

	uint8_t result;

	switch (command) {
	case CMD_CREATE_USER:
		result = cmd_create_user(req);
		break;
	case CMD_DROP_USER:
		result = cmd_drop_user(req);
		break;
	case CMD_SET_PASSWORD:
		result = cmd_set_password(req, false);
		break;
	case CMD_CHANGE_PASSWORD:
		result = cmd_set_password(req, self_ok);
		break;
	case CMD_GRANT_ROLES:
		result = cmd_modify_roles(req, true);
		break;
	case CMD_REVOKE_ROLES:
		result = cmd_modify_roles(req, false);
		break;
	case CMD_QUERY_USERS:
		return cmd_query_users(req, db);
	case CMD_CREATE_ROLE:
		result = cmd_create_role(req);
		break;
	case CMD_DROP_ROLE:
		result = cmd_drop_role(req);
		break;
	case CMD_GRANT_PRIVILEGES:
		result = cmd_modify_privs(req, true);
		break;
	case CMD_REVOKE_PRIVILEGES:
		result = cmd_modify_privs(req, false);
		break;
	case CMD_QUERY_ROLES:
		return cmd_query_roles(req, db);
	case CMD_SET_WHITELIST:
		result = AS_SEC_ERR_WHITELIST; // not yet implemented (P1b)
		break;
	case CMD_SET_QUOTAS:
		result = AS_SEC_ERR_QUOTAS_NOT_ENABLED;
		break;
	default:
		result = AS_SEC_ERR_COMMAND;
		break;
	}

	if (g_config.sec_cfg.report.user_admin ||
			(result != AS_OK && g_config.sec_cfg.report.violation)) {
		cf_info(AS_SECURITY, "permitted true | client: %s | user: %s | action: %s | detail: %s | result %u",
				fd_h->client, self, command_name(command),
				req->fields[FIELD_USER].present ? target : "", result);
	}

	if (result == AS_OK) {
		append_msg_header(db, AS_OK, 0);
	}

	return result;
}

static uint8_t
cmd_login(sec_conn* conn, const sec_request* req, cf_dyn_buf* db)
{
	char name[MAX_USER_SIZE];
	const sec_field* cred = &req->fields[FIELD_CREDENTIAL];

	if (! field_string(req, FIELD_USER, name, sizeof(name))) {
		return AS_SEC_ERR_USER;
	}

	if (! cred->present || cred->sz < MIN_CREDENTIAL_SIZE ||
			cred->sz > MAX_CREDENTIAL_SIZE) {
		return AS_SEC_ERR_CREDENTIAL;
	}

	pthread_rwlock_rdlock(&g_lock);

	sec_user* found = find_user(name);
	sec_user* user = found != NULL ? user_dup(found) : NULL;

	pthread_rwlock_unlock(&g_lock);

	uint8_t result = user == NULL ? AS_SEC_ERR_USER :
			(verify_credential(user, cred->data, cred->sz) ?
					AS_OK : AS_SEC_ERR_CREDENTIAL);

	if (g_config.sec_cfg.report.authentication ||
			(result != AS_OK && g_config.sec_cfg.report.violation)) {
		cf_info(AS_SECURITY, "login %s | user: %s | result %u",
				result == AS_OK ? "succeeded" : "failed", name, result);
	}

	if (result != AS_OK) {
		if (user != NULL) {
			user_free(user);
		}

		return result;
	}

	uint32_t ttl = g_config.sec_cfg.session_ttl;
	uint8_t token[TOKEN_SZ];

	make_token(user, (uint64_t)time(NULL) + ttl, token);

	cf_mutex_lock(&conn->lock);
	conn->authenticated = true;
	conn->gen = 0; // force privilege resolution
	strcpy(conn->user, user->name);
	memcpy(conn->hash, user->hash, HASH_SZ);
	cf_mutex_unlock(&conn->lock);

	user_free(user);

	uint32_t ttl_be = htonl(ttl);

	append_msg_header(db, AS_OK, 2);
	append_field(db, FIELD_SESSION_TOKEN, token, TOKEN_SZ);
	append_field(db, FIELD_SESSION_TTL, &ttl_be, sizeof(ttl_be));

	return AS_OK;
}

static uint8_t
cmd_authenticate(sec_conn* conn, const sec_request* req)
{
	char name[MAX_USER_SIZE];
	const sec_field* token = &req->fields[FIELD_SESSION_TOKEN];

	if (! field_string(req, FIELD_USER, name, sizeof(name))) {
		return AS_SEC_ERR_USER;
	}

	if (! token->present) {
		return AS_SEC_ERR_CREDENTIAL;
	}

	pthread_rwlock_rdlock(&g_lock);

	sec_user* found = find_user(name);
	sec_user* user = found != NULL ? user_dup(found) : NULL;

	pthread_rwlock_unlock(&g_lock);

	if (user == NULL) {
		return AS_SEC_ERR_USER;
	}

	uint8_t result = verify_token(user, token->data, token->sz);

	if (result == AS_OK) {
		cf_mutex_lock(&conn->lock);
		conn->authenticated = true;
		conn->gen = 0;
		strcpy(conn->user, user->name);
		memcpy(conn->hash, user->hash, HASH_SZ);
		cf_mutex_unlock(&conn->lock);
	}
	else if (g_config.sec_cfg.report.violation) {
		cf_info(AS_SECURITY, "authenticate failed | user: %s | result %u",
				name, result);
	}

	user_free(user);

	return result;
}

static uint8_t
cmd_create_user(const sec_request* req)
{
	char name[MAX_USER_SIZE];
	const sec_field* cred = &req->fields[FIELD_PASSWORD];

	if (! field_string(req, FIELD_USER, name, sizeof(name)) ||
			! valid_name(name)) {
		return AS_SEC_ERR_USER;
	}

	if (! cred->present || cred->sz < MIN_CREDENTIAL_SIZE ||
			cred->sz > MAX_CREDENTIAL_SIZE) {
		return AS_SEC_ERR_PASSWORD;
	}

	sec_user user = { 0 };
	uint8_t result = AS_OK;

	strcpy(user.name, name);

	if (req->fields[FIELD_ROLES].present) {
		result = parse_roles(&req->fields[FIELD_ROLES], &user.n_roles,
				&user.roles);

		if (result != AS_OK) {
			return result;
		}
	}

	pthread_rwlock_rdlock(&g_lock);

	if (find_user(name) != NULL) {
		result = AS_SEC_ERR_USER_EXISTS;
	}

	for (uint32_t i = 0; result == AS_OK && i < user.n_roles; i++) {
		if (! role_exists(user.roles[i])) {
			result = AS_SEC_ERR_ROLE;
		}
	}

	pthread_rwlock_unlock(&g_lock);

	if (result == AS_OK) {
		RAND_bytes(user.salt, SALT_SZ);
		hash_credential(cred->data, cred->sz, user.salt, user.hash);

		if (! smd_set_user(&user)) {
			result = AS_ERR_UNKNOWN;
		}
	}

	cf_free(user.roles);

	return result;
}

static uint8_t
cmd_drop_user(const sec_request* req)
{
	char name[MAX_USER_SIZE];

	if (! field_string(req, FIELD_USER, name, sizeof(name))) {
		return AS_SEC_ERR_USER;
	}

	pthread_rwlock_rdlock(&g_lock);

	bool exists = find_user(name) != NULL;

	pthread_rwlock_unlock(&g_lock);

	if (! exists) {
		return AS_SEC_ERR_USER;
	}

	return smd_delete(SMD_USER_PREFIX, name) ? AS_OK : AS_ERR_UNKNOWN;
}

// SET_PASSWORD (user-admin) sets anyone's password. CHANGE_PASSWORD on oneself
// must also present the old credential.
static uint8_t
cmd_set_password(const sec_request* req, bool check_old)
{
	char name[MAX_USER_SIZE];
	const sec_field* cred = &req->fields[FIELD_PASSWORD];
	const sec_field* old = &req->fields[FIELD_OLD_PASSWORD];

	if (! field_string(req, FIELD_USER, name, sizeof(name))) {
		return AS_SEC_ERR_USER;
	}

	if (! cred->present || cred->sz < MIN_CREDENTIAL_SIZE ||
			cred->sz > MAX_CREDENTIAL_SIZE) {
		return AS_SEC_ERR_PASSWORD;
	}

	pthread_rwlock_rdlock(&g_lock);

	sec_user* found = find_user(name);
	sec_user* user = found != NULL ? user_dup(found) : NULL;

	pthread_rwlock_unlock(&g_lock);

	if (user == NULL) {
		return AS_SEC_ERR_USER;
	}

	uint8_t result = AS_OK;

	if (check_old && (! old->present ||
			! verify_credential(user, old->data, old->sz))) {
		result = AS_SEC_ERR_PASSWORD;
	}

	if (result == AS_OK) {
		RAND_bytes(user->salt, SALT_SZ);
		hash_credential(cred->data, cred->sz, user->salt, user->hash);

		if (! smd_set_user(user)) {
			result = AS_ERR_UNKNOWN;
		}
	}

	user_free(user);

	return result;
}

static uint8_t
cmd_modify_roles(const sec_request* req, bool grant)
{
	char name[MAX_USER_SIZE];

	if (! field_string(req, FIELD_USER, name, sizeof(name))) {
		return AS_SEC_ERR_USER;
	}

	uint32_t n_roles;
	char (*roles)[MAX_ROLE_SIZE];
	uint8_t result = parse_roles(&req->fields[FIELD_ROLES], &n_roles, &roles);

	if (result != AS_OK) {
		return result;
	}

	pthread_rwlock_rdlock(&g_lock);

	sec_user* found = find_user(name);
	sec_user* user = found != NULL ? user_dup(found) : NULL;

	for (uint32_t i = 0; grant && i < n_roles; i++) {
		if (! role_exists(roles[i])) {
			result = AS_SEC_ERR_ROLE;
		}
	}

	pthread_rwlock_unlock(&g_lock);

	if (user == NULL) {
		cf_free(roles);
		return AS_SEC_ERR_USER;
	}

	if (result == AS_OK) {
		// Rebuild the role list - at most old + new entries.
		char (*out)[MAX_ROLE_SIZE] = cf_calloc(user->n_roles + n_roles + 1,
				MAX_ROLE_SIZE);
		uint32_t n_out = 0;

		for (uint32_t i = 0; i < user->n_roles; i++) {
			bool revoked = false;

			for (uint32_t j = 0; ! grant && j < n_roles; j++) {
				revoked = revoked || strcmp(user->roles[i], roles[j]) == 0;
			}

			if (! revoked) {
				strcpy(out[n_out++], user->roles[i]);
			}
		}

		for (uint32_t j = 0; grant && j < n_roles; j++) {
			bool have = false;

			for (uint32_t i = 0; i < n_out; i++) {
				have = have || strcmp(out[i], roles[j]) == 0;
			}

			if (! have) {
				strcpy(out[n_out++], roles[j]);
			}
		}

		if (n_out > MAX_ROLES_PER_USER) {
			result = AS_SEC_ERR_ROLE;
		}
		else {
			cf_free(user->roles);
			user->roles = out;
			user->n_roles = n_out;
			out = NULL;

			if (! smd_set_user(user)) {
				result = AS_ERR_UNKNOWN;
			}
		}

		cf_free(out);
	}

	cf_free(roles);
	user_free(user);

	return result;
}

static uint8_t
cmd_query_users(const sec_request* req, cf_dyn_buf* db)
{
	char name[MAX_USER_SIZE];
	bool one = field_string(req, FIELD_USER, name, sizeof(name));

	pthread_rwlock_rdlock(&g_lock);

	if (one) {
		const sec_user* user = find_user(name);

		if (user == NULL) {
			pthread_rwlock_unlock(&g_lock);
			return AS_SEC_ERR_USER;
		}

		append_user(db, user);
	}
	else {
		cf_vector users;

		cf_vector_init(&users, sizeof(sec_user*), 64, 0);
		cf_shash_reduce(g_users, collect_reduce_cb, &users);

		for (uint32_t i = 0; i < cf_vector_size(&users); i++) {
			const sec_user* user = *(sec_user**)cf_vector_getp(&users, i);

			append_user(db, user);
		}

		cf_vector_destroy(&users);
	}

	pthread_rwlock_unlock(&g_lock);

	append_msg_header(db, AS_SEC_OK_LAST, 0);

	return AS_OK;
}

static uint8_t
cmd_create_role(const sec_request* req)
{
	char name[MAX_ROLE_SIZE];

	if (! field_string(req, FIELD_ROLE, name, sizeof(name)) ||
			! valid_name(name)) {
		return AS_SEC_ERR_ROLE;
	}

	if (req->fields[FIELD_WHITELIST].present &&
			req->fields[FIELD_WHITELIST].sz != 0) {
		return AS_SEC_ERR_WHITELIST; // not yet implemented (P1b)
	}

	if (req->fields[FIELD_READ_QUOTA].present ||
			req->fields[FIELD_WRITE_QUOTA].present) {
		return AS_SEC_ERR_QUOTAS_NOT_ENABLED;
	}

	sec_role role = { 0 };
	uint8_t result = AS_OK;

	strcpy(role.name, name);

	if (req->fields[FIELD_PRIVILEGES].present) {
		result = parse_privs(&req->fields[FIELD_PRIVILEGES], &role.n_privs,
				&role.privs);

		if (result != AS_OK) {
			return result;
		}
	}

	pthread_rwlock_rdlock(&g_lock);

	if (role_exists(name)) {
		result = AS_SEC_ERR_ROLE_EXISTS;
	}

	pthread_rwlock_unlock(&g_lock);

	if (result == AS_OK && ! smd_set_role(&role)) {
		result = AS_ERR_UNKNOWN;
	}

	cf_free(role.privs);

	return result;
}

static uint8_t
cmd_drop_role(const sec_request* req)
{
	char name[MAX_ROLE_SIZE];

	if (! field_string(req, FIELD_ROLE, name, sizeof(name))) {
		return AS_SEC_ERR_ROLE;
	}

	pthread_rwlock_rdlock(&g_lock);

	bool exists = find_role(name) != NULL;

	pthread_rwlock_unlock(&g_lock);

	// Predefined roles can't be dropped. Users keep a dropped role's name, but
	// it confers nothing (and a re-created role with that name applies again).
	if (! exists) {
		return AS_SEC_ERR_ROLE;
	}

	return smd_delete(SMD_ROLE_PREFIX, name) ? AS_OK : AS_ERR_UNKNOWN;
}

static bool
priv_equal(const sec_priv* a, const sec_priv* b)
{
	return a->code == b->code && strcmp(a->ns, b->ns) == 0 &&
			strcmp(a->set, b->set) == 0;
}

static uint8_t
cmd_modify_privs(const sec_request* req, bool grant)
{
	char name[MAX_ROLE_SIZE];

	if (! field_string(req, FIELD_ROLE, name, sizeof(name))) {
		return AS_SEC_ERR_ROLE;
	}

	uint32_t n_privs;
	sec_priv* privs;
	uint8_t result = parse_privs(&req->fields[FIELD_PRIVILEGES], &n_privs,
			&privs);

	if (result != AS_OK) {
		return result;
	}

	pthread_rwlock_rdlock(&g_lock);

	sec_role* found = find_role(name);
	sec_role* role = found != NULL ? role_dup(found) : NULL;

	pthread_rwlock_unlock(&g_lock);

	if (role == NULL) {
		cf_free(privs);
		return AS_SEC_ERR_ROLE; // includes predefined roles
	}

	sec_priv* out = cf_calloc(role->n_privs + n_privs + 1, sizeof(sec_priv));
	uint32_t n_out = 0;

	for (uint32_t i = 0; i < role->n_privs; i++) {
		bool revoked = false;

		for (uint32_t j = 0; ! grant && j < n_privs; j++) {
			revoked = revoked || priv_equal(&role->privs[i], &privs[j]);
		}

		if (! revoked) {
			out[n_out++] = role->privs[i];
		}
	}

	for (uint32_t j = 0; grant && j < n_privs; j++) {
		bool have = false;

		for (uint32_t i = 0; i < n_out; i++) {
			have = have || priv_equal(&out[i], &privs[j]);
		}

		if (! have) {
			out[n_out++] = privs[j];
		}
	}

	if (n_out > MAX_PRIVS_PER_ROLE) {
		result = AS_SEC_ERR_PRIVILEGE;
		cf_free(out);
	}
	else {
		cf_free(role->privs);
		role->privs = out;
		role->n_privs = n_out;

		if (! smd_set_role(role)) {
			result = AS_ERR_UNKNOWN;
		}
	}

	cf_free(privs);
	role_free(role);

	return result;
}

static uint8_t
cmd_query_roles(const sec_request* req, cf_dyn_buf* db)
{
	char name[MAX_ROLE_SIZE];
	bool one = field_string(req, FIELD_ROLE, name, sizeof(name));

	pthread_rwlock_rdlock(&g_lock);

	for (uint32_t i = 0; i < N_PREDEFINED_ROLES; i++) {
		const predefined_role* pre = &PREDEFINED_ROLES[i];

		if (! one || strcmp(pre->name, name) == 0) {
			sec_priv priv = { .code = pre->code };

			append_role(db, pre->name, 1, &priv);
		}
	}

	cf_vector roles;

	cf_vector_init(&roles, sizeof(sec_role*), 64, 0);
	cf_shash_reduce(g_roles, collect_reduce_cb, &roles);

	bool found = one && find_predefined_role(name) != NULL;

	for (uint32_t i = 0; i < cf_vector_size(&roles); i++) {
		const sec_role* role = *(sec_role**)cf_vector_getp(&roles, i);

		if (! one || strcmp(role->name, name) == 0) {
			append_role(db, role->name, role->n_privs, role->privs);
			found = true;
		}
	}

	cf_vector_destroy(&roles);

	pthread_rwlock_unlock(&g_lock);

	if (one && ! found) {
		return AS_SEC_ERR_ROLE;
	}

	append_msg_header(db, AS_SEC_OK_LAST, 0);

	return AS_OK;
}


//==========================================================
// Local helpers - responses.
//

static void
append_msg_header(cf_dyn_buf* db, uint8_t result, uint8_t n_fields)
{
	as_sec_msg m = {
			.scheme = AS_SEC_MSG_SCHEME,
			.result = result,
			.n_fields = n_fields
	};

	cf_dyn_buf_append_buf(db, (uint8_t*)&m, sizeof(m));
}

static void
append_field(cf_dyn_buf* db, uint8_t id, const void* data, uint32_t sz)
{
	uint32_t len_be = htonl(sz + 1);

	cf_dyn_buf_append_buf(db, (uint8_t*)&len_be, sizeof(len_be));
	cf_dyn_buf_append_char(db, (char)id);
	cf_dyn_buf_append_buf(db, (const uint8_t*)data, sz);
}

static void
append_user(cf_dyn_buf* db, const sec_user* user)
{
	append_msg_header(db, AS_OK, 2);
	append_field(db, FIELD_USER, user->name, (uint32_t)strlen(user->name));

	// Roles - count byte, then a length byte and name per role.
	uint32_t sz = 1;

	for (uint32_t i = 0; i < user->n_roles; i++) {
		sz += 1 + (uint32_t)strlen(user->roles[i]);
	}

	uint32_t len_be = htonl(sz + 1);

	cf_dyn_buf_append_buf(db, (uint8_t*)&len_be, sizeof(len_be));
	cf_dyn_buf_append_char(db, FIELD_ROLES);
	cf_dyn_buf_append_char(db, (char)user->n_roles);

	for (uint32_t i = 0; i < user->n_roles; i++) {
		uint8_t len = (uint8_t)strlen(user->roles[i]);

		cf_dyn_buf_append_char(db, (char)len);
		cf_dyn_buf_append_buf(db, (const uint8_t*)user->roles[i], len);
	}
}

static void
append_role(cf_dyn_buf* db, const char* name, uint32_t n_privs,
		const sec_priv* privs)
{
	append_msg_header(db, AS_OK, 2);
	append_field(db, FIELD_ROLE, name, (uint32_t)strlen(name));

	// Privileges - count byte, then code (and ns & set for data privileges).
	uint32_t sz = 1;

	for (uint32_t i = 0; i < n_privs; i++) {
		sz += 1;

		if (privs[i].code >= PRIV_READ) {
			sz += 2 + (uint32_t)strlen(privs[i].ns) +
					(uint32_t)strlen(privs[i].set);
		}
	}

	uint32_t len_be = htonl(sz + 1);

	cf_dyn_buf_append_buf(db, (uint8_t*)&len_be, sizeof(len_be));
	cf_dyn_buf_append_char(db, FIELD_PRIVILEGES);
	cf_dyn_buf_append_char(db, (char)n_privs);

	for (uint32_t i = 0; i < n_privs; i++) {
		cf_dyn_buf_append_char(db, (char)privs[i].code);

		if (privs[i].code >= PRIV_READ) {
			uint8_t ns_len = (uint8_t)strlen(privs[i].ns);
			uint8_t set_len = (uint8_t)strlen(privs[i].set);

			cf_dyn_buf_append_char(db, (char)ns_len);
			cf_dyn_buf_append_buf(db, (const uint8_t*)privs[i].ns, ns_len);
			cf_dyn_buf_append_char(db, (char)set_len);
			cf_dyn_buf_append_buf(db, (const uint8_t*)privs[i].set, set_len);
		}
	}
}

static void
send_response(as_transaction* tr, cf_dyn_buf* db)
{
	as_proto* proto = (as_proto*)db->buf;

	proto->version = PROTO_VERSION;
	proto->type = PROTO_TYPE_SECURITY;
	proto->sz = db->used_sz - sizeof(as_proto);
	as_proto_swap(proto);

	as_file_handle* fd_h = tr->from.proto_fd_h;
	cf_socket* sock = &fd_h->sock;

	if (cf_socket_send_all(sock, db->buf, db->used_sz, MSG_NOSIGNAL,
			CF_SOCKET_TIMEOUT) < 0) {
		cf_warning(AS_SECURITY, "fd %d send failed, errno %d", CSFD(sock),
				errno);
		as_end_of_transaction_force_close(fd_h);
		tr->from.proto_fd_h = NULL;
		return;
	}

	as_end_of_transaction_ok(fd_h);
	tr->from.proto_fd_h = NULL;
}


//==========================================================
// Local helpers - logging.
//

static const char*
conn_user(const sec_conn* conn)
{
	// Unlocked read for logging only - user is set once per login.
	return conn != NULL && conn->authenticated ? conn->user : "";
}

static const char*
command_name(uint8_t command)
{
	switch (command) {
	case CMD_AUTHENTICATE: return "authenticate";
	case CMD_CREATE_USER: return "create user";
	case CMD_DROP_USER: return "drop user";
	case CMD_SET_PASSWORD: return "set password";
	case CMD_CHANGE_PASSWORD: return "change password";
	case CMD_GRANT_ROLES: return "grant roles";
	case CMD_REVOKE_ROLES: return "revoke roles";
	case CMD_QUERY_USERS: return "query users";
	case CMD_CREATE_ROLE: return "create role";
	case CMD_DROP_ROLE: return "drop role";
	case CMD_GRANT_PRIVILEGES: return "grant privileges";
	case CMD_REVOKE_PRIVILEGES: return "revoke privileges";
	case CMD_SET_WHITELIST: return "set whitelist";
	case CMD_SET_QUOTAS: return "set quotas";
	case CMD_QUERY_ROLES: return "query roles";
	case CMD_LOGIN: return "login";
	default: return "unknown";
	}
}
