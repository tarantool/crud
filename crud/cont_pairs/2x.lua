local ffi = require('ffi')
local fun = require('fun')

ffi.cdef(
[[
typedef struct tuple_format box_tuple_format_t;
typedef struct tuple box_tuple_t;

box_tuple_format_t *
box_tuple_format(box_tuple_t *tuple);

box_tuple_t *
box_tuple_new(box_tuple_format_t *format, const char *data,
              const char *end);

int
box_tuple_ref(box_tuple_t *tuple);
void
box_tuple_unref(box_tuple_t *tuple);

typedef uint64_t hint_t;

/** @copydoc tuple_compare_with_key() */
typedef int (*tuple_compare_with_key_t)(struct tuple *tuple,
					hint_t tuple_hint,
					const char *key,
					uint32_t part_count,
					hint_t key_hint,
					struct key_def *key_def);
/** @copydoc tuple_compare() */
typedef int (*tuple_compare_t)(struct tuple *tuple_a,
			       hint_t tuple_a_hint,
			       struct tuple *tuple_b,
			       hint_t tuple_b_hint,
			       struct key_def *key_def);
/** @copydoc tuple_extract_key() */
typedef char *(*tuple_extract_key_t)(struct tuple *tuple,
				     struct key_def *key_def,
				     int multikey_idx,
				     uint32_t *key_size);
/** @copydoc tuple_extract_key_raw() */
typedef char *(*tuple_extract_key_raw_t)(const char *data,
					 const char *data_end,
					 struct key_def *key_def,
					 int multikey_idx,
					 uint32_t *key_size);
/** @copydoc tuple_hash() */
typedef uint32_t (*tuple_hash_t)(struct tuple *tuple,
				 struct key_def *key_def);
/** @copydoc key_hash() */
typedef uint32_t (*key_hash_t)(const char *key,
				struct key_def *key_def);
/** @copydoc tuple_hint() */
typedef hint_t (*tuple_hint_t)(struct tuple *tuple,
			       struct key_def *key_def);
/** @copydoc key_hint() */
typedef hint_t (*key_hint_t)(const char *key, uint32_t part_count,
			     struct key_def *key_def);

/* Definition of a multipart key. */
struct key_def {
	/** @see tuple_compare() */
	tuple_compare_t tuple_compare;
	/** @see tuple_compare_with_key() */
	tuple_compare_with_key_t tuple_compare_with_key;
	/** @see tuple_extract_key() */
	tuple_extract_key_t tuple_extract_key;
	/** @see tuple_extract_key_raw() */
	tuple_extract_key_raw_t tuple_extract_key_raw;
	/** @see tuple_hash() */
	tuple_hash_t tuple_hash;
	/** @see key_hash() */
	key_hash_t key_hash;
	/** @see tuple_hint() */
	tuple_hint_t tuple_hint;
	/** @see key_hint() */
	key_hint_t key_hint;
	/**
	 * Minimal part count which always is unique. For example,
	 * if a secondary index is unique, then
	 * unique_part_count == secondary index part count. But if
	 * a secondary index is not unique, then
	 * unique_part_count == part count of a merged key_def.
	 */
	uint32_t unique_part_count;
	/** True, if at least one part can store NULL. */
	bool is_nullable;
	/** True if some key part has JSON path. */
	bool has_json_paths;
	/** True if it is a multikey index definition.
	 * XXX Not used for multikey functional indexes,
	 * please use func->def.is_multikey instead.
	 */
	bool is_multikey;
	/** True if it is a functional index key definition. */
	bool for_func_index;
	/**
	 * True, if some key parts can be absent in a tuple. These
	 * fields assumed to be MP_NIL.
	 */
	bool has_optional_parts;
	/** Key fields mask. @sa column_mask.h for details. */
	uint64_t column_mask;
	/**
	 * A pointer to a functional index function.
	 * Initially set to NULL and is initialized when the
	 * record in _func_index is handled by a respective trigger.
	 * The reason is that we may not yet have a defined
	 * function when a functional index is defined. E.g.
	 * during recovery, we recovery _index first, and _func
	 * second, so when recovering _index no func object is
	 * loaded in the cache and nothing can be assigned.
	 * Once a pointer is assigned its life cycle is guarded by
	 * a check in _func on_replace trigger in alter.cc which
	 * would not let anyone change a function until it is
	 * referenced by a functional index.
	 * In future, one will be able to update a function of
	 * a functional index by disabling the index, thus
	 * clearing this pointer, modifying the function, and
	 * enabling/rebuilding the index.
	 */
	struct func *func_index_func;
	/**
	 * In case of the multikey index, a pointer to the
	 * JSON path string, the path to the root node of
	 * multikey index that contains the array having
	 * index placeholder sign [*].
	 *
	 * This pointer duplicates the JSON path of some key_part.
	 * This path is not 0-terminated. Moreover, it is only
	 * JSON path subpath so key_def::multikey_path_len must
	 * be directly used in all cases.
	 *
	 * This field is not NULL iff this is multikey index
	 * key definition.
	 */
	const char *multikey_path;
	/**
	 * The length of the key_def::multikey_path.
	 * Valid when key_def->is_multikey is true,
	 * undefined otherwise.
	 */
	uint32_t multikey_path_len;
	/**
	 * The index of the root field of the multikey JSON
	 * path index key_def::multikey_path.
	 * Valid when key_def->is_multikey is true,
	 * undefined otherwise.
	*/
	uint32_t multikey_fieldno;
	/** The size of the 'parts' array. */
	uint32_t part_count;
	/** Description of parts of a multipart index. */
	//struct key_part parts[];
};

enum index_type {
	HASH = 0, /* HASH Index */
	TREE,     /* TREE Index */
	BITSET,   /* BITSET Index */
	RTREE,    /* R-Tree Index */
	index_type_MAX,
};

enum rtree_index_distance_type {
	 /* Euclid distance, sqrt(dx*dx + dy*dy) */
	RTREE_INDEX_DISTANCE_TYPE_EUCLID,
	/* Manhattan distance, fabs(dx) + fabs(dy) */
	RTREE_INDEX_DISTANCE_TYPE_MANHATTAN,
	rtree_index_distance_type_MAX
};

/** Index options */
struct index_opts {
	/**
	 * Is this index unique or not - relevant to HASH/TREE
	 * index
	 */
	bool is_unique;
	/**
	 * RTREE index dimension.
	 */
	int64_t dimension;
	/**
	 * RTREE distance type.
	 */
	enum rtree_index_distance_type distance;
	/**
	 * Vinyl index options.
	 */
	int64_t range_size;
	int64_t page_size;
	/**
	 * Maximal number of runs that can be created in a level
	 * of the LSM tree before triggering compaction.
	 */
	int64_t run_count_per_level;
	/**
	 * The LSM tree multiplier. Each subsequent level of
	 * the LSM tree is run_size_ratio times larger than
	 * previous one.
	 */
	double run_size_ratio;
	/* Bloom filter false positive rate. */
	double bloom_fpr;
	/**
	 * LSN from the time of index creation.
	 */
	int64_t lsn;
	/**
	 * SQL specific statistics concerning tuples
	 * distribution for query planer. It is automatically
	 * filled after running ANALYZE command.
	 */
	struct index_stat *stat;
	/** Identifier of the functional index function. */
	uint32_t func_id;
};

struct rlist {
	struct rlist *prev;
	struct rlist *next;
};

/* Definition of an index. */
struct index_def {
	/* A link in key list. */
	struct rlist link;
	/** Ordinal index number in the index array. */
	uint32_t iid;
	/* Space id. */
	uint32_t space_id;
	/** Index name. */
	char *name;
	/** Index type. */
	enum index_type type;
	struct index_opts opts;

	/* Index key definition. */
	struct key_def *key_def;
	/**
	 * User-defined key definition, merged with the primary
	 * key parts. Used by non-unique keys to uniquely identify
	 * iterator position.
	 */
	struct key_def *cmp_def;
};

struct index {
	/** Virtual function table. */
	const struct index_vtab *vtab;
	/** Engine used by this index. */
	struct engine *engine;
	/* Description of a possibly multipart key. */
	struct index_def *def;
	/** Reference counter. */
	int refs;
	/* Space cache version at the time of construction. */
	uint32_t space_cache_version;
};

struct iterator {
    /**
     * Iterate to the next tuple.
     * The tuple is returned in @ret (NULL if EOF).
     * Returns 0 on success, -1 on error.
     */
    int (*next)(struct iterator *it, struct tuple **ret);
    /** Destroy the iterator. */
    void (*free)(struct iterator *);
    /** Space cache version at the time of the last index lookup. */
    uint32_t space_cache_version;
    /** ID of the space the iterator is for. */
    uint32_t space_id;
    /** ID of the index the iterator is for. */
    uint32_t index_id;
    /**
     * Pointer to the index the iterator is for.
     * Guaranteed to be valid only if the schema
     * state has not changed since the last lookup.
     */
    struct index *index;
};

struct matras_view {
    /* root extent of the view */
    void *root;
    /* block count in the view */
    uint32_t block_count;
    /* all views are linked into doubly linked list */
    struct matras_view *prev_view, *next_view;
};

struct memtx_tree_iterator {
    /* ID of a block, containing element. -1 for an invalid iterator */
    uint32_t block_id;
    /* Position of an element in the block. Could be -1 for last in block*/
    uint16_t pos;
    /* Version of matras memory for MVCC */
    struct matras_view view;
};

struct memtx_tree_key_data {
    /** Sequence of msgpacked search fields. */
    const char *key;
    /** Number of msgpacked search fields. */
    uint32_t part_count;
    /** Comparison hint, see tuple_hint(). */
    hint_t hint;
};

struct memtx_tree_data {
    /* Tuple that this node is represents. */
    struct tuple *tuple;
    /** Comparison hint, see key_hint(). */
    hint_t hint;
};

enum iterator_type {
    /* ITER_EQ must be the first member for request_create  */
    ITER_EQ               =  0, /* key == x ASC order                  */
    ITER_REQ              =  1, /* key == x DESC order                 */
    ITER_ALL              =  2, /* all tuples                          */
    ITER_LT               =  3, /* key <  x                            */
    ITER_LE               =  4, /* key <= x                            */
    ITER_GE               =  5, /* key >= x                            */
    ITER_GT               =  6, /* key >  x                            */
    ITER_BITS_ALL_SET     =  7, /* all bits from x are set in key      */
    ITER_BITS_ANY_SET     =  8, /* at least one x's bit is set         */
    ITER_BITS_ALL_NOT_SET =  9, /* all bits are not set                */
    ITER_OVERLAPS         = 10, /* key overlaps x                      */
    ITER_NEIGHBOR         = 11, /* tuples in distance ascending order from specified point */
    iterator_type_MAX
};

struct tree_iterator {
    struct iterator base;
    struct memtx_tree_iterator tree_iterator;
    enum iterator_type type;
    struct memtx_tree_key_data key_data;
    struct memtx_tree_data current;
    /** Memory pool the iterator was allocated from. */
    struct mempool *pool;
};

]]
)

local index_cont_pairs = function(index, key, last_tuple, opts)
    local gen,param,state = index:pairs(key, opts)
    if not last_tuple then
        return gen,param,state
    end
    local t0 = index:min()
    if not t0 then
        return gen,param,state
    end

    -- TODO: need more checks of last_tuple to be compatible with the space
    -- TODO: check that engine is memtx and index is tree

    local fmt = ffi.C.box_tuple_format(t0)
    local data_start, data_end = box.internal.tuple.encode(last_tuple)
    local tuple = ffi.C.box_tuple_new(fmt, data_start, data_end)
    ffi.C.box_tuple_ref(tuple)

    local t
    state, t = gen(param, state)
    if t == nil then
        ffi.C.box_tuple_unref(tuple)
        return fun.iter({})
    end

    if not (ffi.cast("struct tree_iterator&", state).current.tuple == box.NULL) then
        ffi.C.box_tuple_unref(ffi.cast("struct tree_iterator&", state).current.tuple)
    end
    ffi.cast("struct tree_iterator&", state).current.tuple = tuple
    local def = ffi.cast("struct tree_iterator&", state).base.index.def.cmp_def
    local hint = def.tuple_hint(tuple, def)
    ffi.cast("struct tree_iterator&", state).current.hint = hint

    return gen,param,state
end

local space_cont_pairs = function(space, key, last_tuple, opts)
    box.internal.check_space_arg(space, 'pairs')
    local pk = space.index[0]
    if pk == nil then
        -- empty space without indexes, return empty iterator
        return fun.iter({})
    end
    return index_cont_pairs(pk, key, last_tuple, opts)
end

box.schema.index_mt['cont_pairs'] = index_cont_pairs
box.schema.space_mt['cont_pairs'] = space_cont_pairs
