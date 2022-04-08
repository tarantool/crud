-- It is not clear how to describe modules
-- with constants for ldoc. ldoc-styled description
-- for this module is available at `crud.stats.init`.
-- See https://github.com/lunarmodules/LDoc/issues/369
-- for possible updates.
return {
    -- INSERT identifies both `insert` and `insert_object`.
    INSERT = 'insert',
    -- INSERT_MANY identifies both `insert_many` and `insert_object_many`.
    INSERT_MANY = 'insert_many',
    GET = 'get',
    -- REPLACE identifies both `replace` and `replace_object`.
    REPLACE = 'replace',
    -- REPLACE_MANY identifies both `replace_many` and `replace_object_many`.
    REPLACE_MANY = 'replace_many',
    UPDATE = 'update',
    -- UPSERT identifies both `upsert` and `upsert_object`.
    UPSERT = 'upsert',
    -- UPSERT_MANY identifies both `upsert_many` and `upsert_object_many`.
    UPSERT_MANY = 'upsert_many',
    DELETE = 'delete',
    -- SELECT identifies both `pairs` and `select`.
    SELECT = 'select',
    TRUNCATE = 'truncate',
    LEN = 'len',
    COUNT = 'count',
    -- BORDERS identifies both `min` and `max`.
    BORDERS = 'borders',
}
