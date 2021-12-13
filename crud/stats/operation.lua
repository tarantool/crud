-- It is not clear how to describe modules
-- with constants for ldoc. ldoc-styled description
-- for this module is available at `crud.stats.init`.
-- See https://github.com/lunarmodules/LDoc/issues/369
-- for possible updates.
return {
    -- INSERT identifies both `insert` and `insert_object`.
    INSERT = 'insert',
    GET = 'get',
    -- REPLACE identifies both `replace` and `replace_object`.
    REPLACE = 'replace',
    UPDATE = 'update',
    -- UPSERT identifies both `upsert` and `upsert_object`.
    UPSERT = 'upsert',
    DELETE = 'delete',
    -- SELECT identifies both `pairs` and `select`.
    SELECT = 'select',
    TRUNCATE = 'truncate',
    LEN = 'len',
    COUNT = 'count',
    -- BORDERS identifies both `min` and `max`.
    BORDERS = 'borders',
}
