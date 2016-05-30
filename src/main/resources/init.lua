if #KEYS == 1 then
    redis.call('SET', sequencer, KEYS[1])
    return reply
elseif #KEYS == 0 then
    if redis.call('EXISTS', sequencer) == 0 then
        redis.call('SET', sequencer, 0)
    end
    return reply
else
    return argErr
end