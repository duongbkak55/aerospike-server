/*
 * ext_record.h
 *
 * Tombstone helpers shared by the ext build's record, storage and
 * transaction hooks.
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

//==========================================================
// Forward declarations.
//

struct as_flat_opt_meta_s;
struct as_flat_record_s;
struct as_index_ref_s;
struct as_index_s;
struct as_index_tree_s;
struct as_namespace_s;
struct as_xdr_submit_info_s;

//==========================================================
// Public API.
//

// Adjust namespace, set and partition counters for a record moving between
// live and tombstone. Does not touch r->tombstone - callers set it.
void ext_record_count_transition(const struct as_index_s* r,
		struct as_namespace_s* ns, bool was_tombstone, bool is_tombstone);

// Set or clear r->tombstone and adjust counters accordingly. Does not touch
// the set index - callers handle it.
void ext_record_set_tombstone(struct as_index_s* r, struct as_namespace_s* ns,
		bool tombstone);

// Cold start - a newer version of an indexed record was found on storage. Moves
// the record between live and tombstone, including set index, as needed.
void drv_cold_start_transition_tombstone(struct as_namespace_s* ns,
		const struct as_flat_opt_meta_s* opt_meta, struct as_index_tree_s* tree,
		struct as_index_ref_s* r_ref);

// Tomb raider - 'scan' reads every record on persistent storage and calls
// ext_tomb_raider_check_flat() for each. NULL if storage doesn't persist.
typedef void (*ext_tomb_raider_scan_fn)(struct as_namespace_s* ns);

void ext_tomb_raider_start(struct as_namespace_s* ns,
		ext_tomb_raider_scan_fn scan);
void ext_tomb_raider_check_flat(struct as_namespace_s* ns,
		const struct as_flat_record_s* flat);

// Cold start - a tombstone with no older live copy on storage is a cenotaph.
// adjust: an older version of r was found on storage. drop: remove eligible
// cenotaphs once all storage is read.
void ext_cold_start_adjust_cenotaph(const struct as_flat_record_s* flat,
		struct as_index_s* r);
void ext_cold_start_drop_cenotaphs(struct as_namespace_s* ns);

// Calls ext_tomb_raider_check_flat() for each record in a wblock buffer.
void ext_tomb_raider_check_wblock(struct as_namespace_s* ns,
		const uint8_t* buf);

// Bin convergence - XDR LUTs (the message LUT field and per-op LUTs) carry
// the writing cluster's src-id in the top byte, the LUT in the rest.
#define EXT_XDR_LUT_MASK ((1UL << 56) - 1)

static inline uint64_t
ext_xdr_pack_lut(uint64_t lut, uint8_t src_id)
{
	return ((uint64_t)src_id << 56) | (lut & EXT_XDR_LUT_MASK);
}

static inline uint64_t
ext_xdr_lut(uint64_t packed)
{
	return packed & EXT_XDR_LUT_MASK;
}

static inline uint8_t
ext_xdr_src_id(uint64_t packed)
{
	return (uint8_t)(packed >> 56);
}

// XDR - a non-durable delete on the master. Ships a delete if the record is
// still gone when the ship worker gets to it.
void ext_xdr_submit_drop(const struct as_namespace_s* ns,
		const struct as_xdr_submit_info_s* info);
