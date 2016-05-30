-- ./redis-cli -p 6379 --eval /Users/chengyang/gitrepository/countedis/countedis-core/src/main/resources/test.lua
local kv = ''
for i = 0, 96 do
    local xx = redis.call('LLEN', 'countedis.user.log.' .. i)
    if tonumber(xx) > 0 then
        kv = kv .. i .. ':' .. xx .. ';'
    end

    local xx = redis.call('LLEN', 'countedis.user.log.' .. i .. '.processing')
    if tonumber(xx) > 0 then
        kv = kv .. i .. '__ing:' .. xx .. ';'
    end
end
return kv