local log = require('log')
local monit, httpd, fiber, json
local version = 'v6.0.4'

local data = {
    DataStat        = {},
    QueryStat       = {},
    NetStat         = {},
    ReplicationStat = {}
}

local function GetStat()

    -- DataStat
    data.DataStat.OBJECTS_COUNT = box.space.otrs_cache:count()

    local box_memory = box.info.memory()
    data.DataStat.DATA_SIZE = box_memory.data
    data.DataStat.INDEX_SIZE = box_memory.index

    local box_slab = box.slab.info()
    data.DataStat.QUOTA_USED = box_slab.quota_used
    data.DataStat.QUOTA_FREE = box_slab.quota_size - box_slab.quota_used
    data.DataStat.QUOTA_RATIO = math.floor( (box_slab.quota_used/box_slab.quota_size)*100 )
    data.DataStat.ITEMS_RATIO = math.floor( (box_slab.items_used/box_slab.items_size)*100 )

    -- QueryStat
    local box_stat = box.stat()
    data.QueryStat.SELECT = box_stat.SELECT.total
    data.QueryStat.INSERT = box_stat.INSERT.total
    data.QueryStat.UPSERT = box_stat.UPSERT.total
    data.QueryStat.REPLACE = box_stat.REPLACE.total
    data.QueryStat.UPDATE = box_stat.UPDATE.total
    data.QueryStat.DELETE = box_stat.DELETE.total
    data.QueryStat.CALL = box_stat.CALL.total

    -- NetStat
    local net_stat = box.stat.net()
    data.NetStat.CONNECTIONS = net_stat.CONNECTIONS.total
    data.NetStat.REQUESTS = net_stat.REQUESTS.total
    data.NetStat.SENT = net_stat.SENT.total
    data.NetStat.RECEIVED = net_stat.RECEIVED.total

    -- ReplicaStat
    local replication = box.info.replication
    local nodes = 0
    local max_lag = 0
    local max_idle = 0

    for key,value in pairs(replication) do
        nodes = nodes + 1
        if value.uuid ~= box.info.uuid then
            if ( value.upstream == nil or value.downstream == nil ) then
                max_lag = 999
                max_idle = 999
            elseif ( value.upstream.status == 'follow' and value.downstream.status == 'follow' ) then
                max_lag = math.max( max_lag, math.abs(value.upstream.lag), math.abs(value.downstream.lag) )
                max_idle = math.max( max_idle, math.abs(value.upstream.idle), math.abs(value.downstream.idle) )
            else
                max_lag = 999
                max_idle = 999
            end
        end
    end
    data.ReplicationStat.NODES = nodes
    data.ReplicationStat.LAG = max_lag
    data.ReplicationStat.IDLE = max_idle    
end

local function start(opt)
    json = require('json')
    fiber = require('fiber')

    fiber.set_max_slice({warn = 1.5, err = 3})
    
    local cfg = {
        debug = opt and opt.debug or false,
        port  = opt and opt.port or 8031,
        host  = opt and opt.host or '0.0.0.0'
    }

    httpd = require('http.server').new(cfg.host, cfg.port, {
        log_requests   = cfg.debug,
        log_errors     = cfg.debug,
        charset        = 'utf8',
        display_errors = cfg.debug,
    })

    httpd:route({ path = '/stat' } function(req)
        local response = req:render({ text = json.encode(data) })
        response.headers['content-type'] = 'application/json'
        response.status = 200
        return response
    end)

    monit = fiber.new( function()
        fiber.sleep(0)
        while true do
            GetStat()
            fiber.sleep(10)
        end
    end)

    httpd:start()
    log.info('Stat[' .. version .. '] Started!')
end

local function stop()
    local depends = {'http.server','fiber','json'}

    httpd:stop()
    fiber.kill( monit:id() )

    httpd = nil
    monit = nil
    json  = nil
    fiber = nil

    for _, package in pairs( depends ) do
        package.loaded[ package ] = nil
    end

    log.info('Stat[' .. version .. '] Stopped!')
end

local function stats()
    return data
end

return {
    stop = stop,
    start = start,
    stats = stats,
    version = version
}
