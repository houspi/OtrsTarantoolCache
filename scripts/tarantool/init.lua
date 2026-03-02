local space_name = 'otrs_cache'

function OtrsCacheBeforeReplaceTrigger(old, new, _, op)
    -- op:  ‘INSERT’, ‘DELETE’, ‘UPDATE’, or ‘REPLACE’
    if new == nil then
        return -- DELETE
    end
    if old == nil then
        return new  -- INSERT
    end

    return new
end

box.ctl.on_schema_init(function()
    box.space._space:on_replace(function(old_space, new_space)
        if not old_space and new_space and new_space.name == space_name then
            box.on_commit(function()
                box.space[space_name]:before_replace(OtrsCacheBeforeReplaceTrigger)
            end)
        end
    end)
end)

box.cfg{
    listen              = '*:3301',
    readahead           = 16320;
    io_collect_interval = nil;
    --
    work_dir            = "/opt/otrs/var/tarantool/otrs_cache";

    ----------------------
    -- Memtx configuration
    ----------------------
    memtx_memory         = 20 * 1024 * 1024 * 1024; -- 10Gb
    memtx_min_tuple_size = 128;
    memtx_max_tuple_size = 128 * 1024 * 1024; -- 128Mb
    memtx_dir            = "memtx";
    memtx_allocator      = "small";

    ----------------------
    -- Vinyl configuration
    ----------------------
    vinyl_dir            = "vinyl";
    vinyl_memory         = 2 * 1024 * 1024 * 1024; -- 2Gb
    vinyl_cache          = 2 * 1024 * 1024 * 1024; -- 2Gb
    vinyl_max_tuple_size = 128 * 1024 * 1024; -- 128Mb
    vinyl_read_threads   = 2;
    vinyl_write_threads  = 2;

    ------------------------------
    -- Binary logging and recovery
    ------------------------------
    wal_mode            = "write";
    wal_max_size        = 256 * 1024 * 1024; -- 256Mb
    checkpoint_count    = 3;
    checkpoint_interval = 0; -- disable
    wal_dir             = "wal";

    ----------
    -- Logging
    ----------
    log       = 'otrs_cache.log';
    log_level = 5;
    too_long_threshold = 0.5;

    ------------------------------
    -- Replication
    ------------------------------

    replication = {
        'replicator:password@192.168.0.1:3301',
        'replicator:password@192.168.0.2:3301',
        'replicator:password@192.168.0.3:3301',
    },
    read_only = false
}

box.once(
    "schema-6.0.4", function()
        box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {if_not_exists=true})

        box.schema.user.create('replicator', {password = 'password', if_not_exists = true})
        box.schema.user.grant('replicator', 'read,write,execute', 'universe', nil, {if_not_exists=true})

        box.schema.user.create('otrs', { password = 'secret', if_not_exists = true })
        box.schema.user.grant('otrs', 'create,read,write,execute,alter,drop', 'universe', nil, {if_not_exists=true})

        local space = box.schema.space.create('otrs_cache', {engine='memtx', if_not_exists = true} )
        space:format({
            { name = 'key', type = 'string' },
            { name = 'type', type = 'string' },
            { name = 'value', type = 'string' },
            { name = 'expired', type = 'number' },
        })
        space:create_index('primary', {
            if_not_exists = true,
            type = 'hash',
            parts = {
                {'key', 'string'},
                {'type', 'string'}
            }
        })
        space:create_index('type_idx', {
            if_not_exists = true,
            unique = false,
            type = 'tree',
            parts = {{'type', 'string'}}
        })
        space:create_index('expired_idx', {
            if_not_exists = true,
            unique = false,
            type = 'tree',
            parts = {{'expired', 'number'}}
        })
    end
)

function CleanUpCache( CacheType )
    local cache = box.space.otrs_cache
    box.begin()
    if not ( CacheType == box.NULL ) then
        for _, tuple in cache.index.type_idx:pairs(CacheType, {iterator = "EQ"}) do
            cache:delete({tuple[1], tuple[2]})
        end
    else
        cache:truncate()
    end
    box.commit()
end

function CleanUpExpiredCache( TTL )
    local cache = box.space.otrs_cache
    box.begin()
    for _, tuple in cache.index.expired_idx:pairs(TTL, {iterator = "LT"}) do
        cache:delete({tuple[1], tuple[2]})
    end
    box.commit()
end

stat = require('stat')
stat.init()
