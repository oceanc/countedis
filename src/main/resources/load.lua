if #KEYS > 0 and #KEYS * 2 == #ARGV then
    local underscore = '_'
    local k = KEYS[1]
    local v = tonumber(ARGV[1])
    local e = ARGV[2]
    if (e == underscore) then e = 0 else e = tonumber(e) end

    if (redis.call('EXISTS', k) == 0) then
        local seq = redis.call('GET', sequencer)
        redis.call('HSET', k, 's', seq)
        redis.call('HSET', k, 'v', v)
    end
    if e > 0 then redis.call('EXPIRE', k, e) end
    return reply
else
    return argErr
end