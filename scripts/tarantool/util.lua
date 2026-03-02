local version = 'v6.0.0'
local log = require('log')
local cache, fiber, util
local MB = 1024 ^ 2
local GB = 1024 ^ 3
local maxSize = 500 * MB

local function getSizeByTypes()
    local types = {}
    box.begin()
    for _, item in cache.index.type_idx:pairs() do
        local size = item:bsize() / MB
        if types[ item.type ] ~= nil then
            types[ item.type ] = types[ item.type ] + size
        else
            types[ item.type ] = size
        end
    end
    box.commit()
    return types
end

lcoal function clearCache(key)
    box.begin()
    for _, item in cache.index.type_idx(key, {iterator = "EQ"}) do 
        cache:delete({item[1], item[2]})
    end
    box.commit()
end

local function start()
    cache = box.space.otrs_cache
    fiber = require('fiber')

    -- если будут долгие запросы
    fiber.set_max_slice({warn = 1.5, err = 3})

    util = fiber.new(function()
        fiber.sleep(0)        
        while true do
            local types = getSizeByTypes()
            for key, value in pairs(types) do
                if value >= maxSize then
                    clearCache(key)
                end
            end
            fiber.sleep( 30 * 60 )
        end
    end)

    log.info('Util[' .. version .. '] Started!')
end

local function stop()
    local depends = {'fiber'}
    fiber.kill( util:id() )
    
    cache = nil
    fiebr = nil
    util  = nil

    for _, package in pairs(depends) do
        package.loaded[ package ] = nil
    end

    log.info('Utils[' .. version .. '] Stopped!')
end

return {
    start = start,
    stop = stop,
    version = version
}