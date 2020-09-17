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

struct memtx_tree_key_data
{
	/** Sequence of msgpacked search fields */
	const char *key;
	/** Number of msgpacked search fields */
	uint32_t part_count;
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
	const struct memtx_tree *tree;
	struct index_def *index_def;
	struct memtx_tree_iterator tree_iterator;
	enum iterator_type type;
	struct memtx_tree_key_data key_data;
	struct tuple *current_tuple;
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
    local data_start, data_end = box.tuple.encode(last_tuple)
    local tuple = ffi.C.box_tuple_new(fmt, data_start, data_end)
    ffi.C.box_tuple_ref(tuple)

    state = gen(param, state)
    if not (ffi.cast("struct tree_iterator&", state).current_tuple == box.NULL) then
        ffi.C.box_tuple_unref(ffi.cast("struct tree_iterator&", state).current_tuple)
    end
    ffi.cast("struct tree_iterator&", state).current_tuple = tuple

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
