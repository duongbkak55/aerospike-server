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

//==========================================================
// Forward declarations.
//

struct as_flat_opt_meta_s;
struct as_index_ref_s;
struct as_index_s;
struct as_index_tree_s;
struct as_namespace_s;

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
