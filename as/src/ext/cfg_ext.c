/*
 * cfg_ext.c
 *
 * Copyright (C) 2016-2020 Aerospike, Inc.
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

#include "base/cfg.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "dynbuf.h"
#include "log.h"

#include "base/datamodel.h"


//==========================================================
// Typedefs & constants.
//

// Enterprise-only config items this build provides, or will provide. Set
// 'ready' when the feature behind an item is implemented - until then the item
// is still rejected, so a config can't silently enable a no-op stub.
typedef struct ext_feature_s {
	const char* name; // config item - last path component
	const char* phase; // see docs/ce-ext/PLAN.md
	bool ready;
} ext_feature;

static const ext_feature EXT_FEATURES[] = {
		// Authentication & authorization.
		{ "security", "P1a", true },

		// Durable deletes & tomb raider.
		{ "tomb-raider-eligible-age", "P2b", true },
		{ "tomb-raider-period", "P2b", true },
		{ "tomb-raider-unmark-threads", "P2b", true },
		{ "tomb-raider-sleep", "P2b", true },
		{ "serialize-tomb-raider", "P2b", true },

		// XDR, including active-active bin convergence.
		{ "xdr", "P3a", true },
		{ "conflict-resolve-writes", "P3b", true },
		{ "xdr-bin-tombstone-ttl", "P3b", true },
		// Accepted for compatibility - bin cemeteries are removed by the tomb
		// raider, and bin tombstones by the writes that find them expired.
		{ "xdr-tomb-raider-period", "P3b", true },
		{ "xdr-tomb-raider-threads", "P3b", true }
};

#define N_EXT_FEATURES (sizeof(EXT_FEATURES) / sizeof(ext_feature))


//==========================================================
// Forward declarations.
//

void post_process_namespace(as_namespace* ns);
static const ext_feature* find_feature(const char* name);
static void log_features(void);


//==========================================================
// Public API.
//

void
as_config_init_namespace(as_namespace* ns)
{
}

// TODO - until we have an info split.
bool
as_error_enterprise_only()
{
	return true;
}

// TODO - until we have an info split.
bool
as_error_enterprise_feature_only(const char* name)
{
	cf_crash(AS_CFG, "community edition checking enterprise feature");
	return true;
}

bool
as_config_allows_feature(const char* name)
{
	const ext_feature* feature = find_feature(name);

	return feature != NULL && feature->ready;
}


//==========================================================
// Private API - for enterprise separation only.
//

void
cfg_enterprise_only(const cfg_line* p_line)
{
	const ext_feature* feature = find_feature(p_line->name_tok);

	if (feature == NULL) {
		cf_crash_nostack(AS_CFG, "line %d :: '%s' is enterprise-only",
				p_line->num, p_line->name_tok);
	}

	if (! feature->ready) {
		cf_crash_nostack(AS_CFG, "line %d :: '%s' is not yet implemented (%s)",
				p_line->num, p_line->name_tok, feature->phase);
	}
}

void
cfg_post_process()
{
	// So far, no other context handled.

	for (uint32_t ns_ix = 0; ns_ix < g_config.n_namespaces; ns_ix++) {
		post_process_namespace(g_config.namespaces[ns_ix]);
	}

	log_features();
}


//==========================================================
// Local helpers.
//

void
post_process_namespace(as_namespace* ns)
{
	if (ns->conflict_resolution_policy ==
			AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_UNDEF) {
		ns->conflict_resolution_policy =
				AS_NAMESPACE_CONFLICT_RESOLUTION_POLICY_GENERATION;
	}
}

static const ext_feature*
find_feature(const char* name)
{
	for (size_t i = 0; i < N_EXT_FEATURES; i++) {
		if (strcmp(EXT_FEATURES[i].name, name) == 0) {
			return &EXT_FEATURES[i];
		}
	}

	return NULL;
}

static void
log_features(void)
{
	cf_dyn_buf_define(db);

	for (size_t i = 0; i < N_EXT_FEATURES; i++) {
		if (EXT_FEATURES[i].ready) {
			cf_dyn_buf_append_string(&db, EXT_FEATURES[i].name);
			cf_dyn_buf_append_char(&db, ',');
		}
	}

	cf_dyn_buf_chomp_char(&db, ',');
	cf_dyn_buf_append_char(&db, '\0');

	cf_info(AS_CFG, "ext build - ready config items: %s",
			db.used_sz > 1 ? (const char*)db.buf : "none");

	cf_dyn_buf_free(&db);
}
