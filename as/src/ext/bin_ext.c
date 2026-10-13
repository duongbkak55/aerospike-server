/*
 * bin_ext.c
 *
 * Ext build - bin tombstones and bin metadata for bin convergence
 * (conflict-resolve-writes). Based on base/bin_ce.c.
 *
 * Copyright (C) 2020 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include "citrusleaf/cf_clock.h"

#include "log.h"

#include "base/datamodel.h"
#include "storage/storage.h"

//==========================================================
// Public API.
//

uint8_t
as_bin_get_particle_type(const as_bin* b)
{
	switch (b->state) {
	case AS_BIN_STATE_INUSE_INTEGER:
		return AS_PARTICLE_TYPE_INTEGER;
	case AS_BIN_STATE_INUSE_FLOAT:
		return AS_PARTICLE_TYPE_FLOAT;
	case AS_BIN_STATE_INUSE_BOOL:
		return AS_PARTICLE_TYPE_BOOL;
	case AS_BIN_STATE_INUSE_OTHER:
		return b->particle->type;
	case AS_BIN_STATE_UNUSED:
	default:
		return AS_PARTICLE_TYPE_NULL;
	}
}

// A bin tombstone is stored as a bare AS_PARTICLE_TYPE_NULL particle.
bool
as_bin_particle_is_tombstone(as_particle_type type)
{
	return type == AS_PARTICLE_TYPE_NULL;
}

bool
as_bin_is_tombstone(const as_bin* b)
{
	return b->state == AS_BIN_STATE_TOMBSTONE;
}

bool
as_bin_is_live(const as_bin* b)
{
	return b->state != AS_BIN_STATE_UNUSED &&
			b->state != AS_BIN_STATE_TOMBSTONE;
}

// Keeps the bin's name and metadata. Particles in rd->bins aren't owned by
// the bin here (stack/storage buffers), so there's nothing to destroy.
void
as_bin_set_tombstone(as_bin* b)
{
	b->state = AS_BIN_STATE_TOMBSTONE;
	b->particle = NULL;
}

// Called by writes and UDFs after all ops are applied. With bin convergence,
// also drops bin tombstones older than xdr-bin-tombstone-ttl (a tombstone
// with LUT 0 was just written). Returns true if no live bins remain - with bin
// tombstones left, the record is a bin cemetery.
bool
as_bin_empty_if_all_tombstones(as_storage_rd* rd, bool is_dd)
{
	if (! rd->ns->conflict_resolve_writes) {
		return rd->n_bins == 0;
	}

	uint64_t ttl_ms = rd->ns->xdr_bin_tombstone_ttl_ms;
	uint64_t now = cf_clepoch_milliseconds();
	uint64_t cutoff = ttl_ms != 0 && ttl_ms < now ? now - ttl_ms : 0;
	bool any_live = false;
	uint32_t i = 0;

	while (i < rd->n_bins) {
		as_bin* b = &rd->bins[i];

		if (as_bin_is_tombstone(b) && b->lut != 0 && b->lut < cutoff) {
			as_bin_remove(rd, i); // moves the last bin here - don't advance
			continue;
		}

		if (as_bin_is_live(b)) {
			any_live = true;
		}

		i++;
	}

	return ! any_live;
}

// Called when a bin is (re)written - the new value gets the write's metadata.
// A tombstone keeps its state - core treats it as a non-live bin, so writes
// and modify ops create a value in place.
void
as_bin_clear_meta(as_bin* b)
{
	b->xdr_write = 0;
	b->unused_flags = 0;
	b->lut = 0;
	b->src_id = 0;
}
