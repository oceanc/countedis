-- ./redis-cli -p 6379 --eval effect.lua k1 k2 , v1 argV1 sequence1 expire1 v2 argV2 sequence2 expire2 id timestamp qShardIndex customLog
-- expire could be '_' means persist.
-- qShardIndex, and customLog is option, if provided and customLog could be '_' means ignore.
-- response type is string, format is ok@k1;k2, @k1;k2 is option means ignored key.
if (#KEYS > 0 and #KEYS * 4 == #ARGV - 4) or (#KEYS > 0 and #KEYS * 4 == #ARGV - 2) then
    local execS = {}; local execV = {}; local execE = {}
    local underscore = '_'; local seq = 0; local ignore = ''
    local logStr = ''; local wLogStr = ''; local vLogStr = ''
    for i = 1, #KEYS do
        local idx = 4 * i - 3
        local k = KEYS[i]
        local v = tonumber(ARGV[idx])
        local a = tonumber(ARGV[idx + 1])
        local s = tonumber(ARGV[idx + 2])
        local e = ARGV[idx + 3]
        if (e == underscore) then e = 0 else e = tonumber(e) end

        local oldS = redis.call('HGET', k, 's')
        if oldS == false or tonumber(oldS) < s then
            execS[k] = s; execV[k] = v; execE[k] = e
        else
            ignore = ignore .. k .. ';'
        end
        seq = s
        logStr = logStr .. k .. ':' .. a .. ',' .. v .. ';'
        wLogStr = wLogStr .. k .. ':' .. a .. ';'
        vLogStr = vLogStr .. v .. ';'
    end
    -- build write log & check whether executed
    wLogStr = ARGV[#KEYS * 4 + 1] .. at .. wLogStr .. ARGV[#KEYS * 4 + 2]
    if (#KEYS * 4 == #ARGV - 4 and ARGV[#ARGV] ~= underscore) then
        wLogStr = wLogStr .. delimiter .. ARGV[#ARGV]
    end
    for i = 1, wShard do
        if redis.call('ZSCORE', writeLog .. i, wLogStr) ~= false then return repeatExec end
    end
    -- update sequnecer & k/v
    local oldSeq = redis.call('GET', sequencer)
    if oldSeq == false or tonumber(oldSeq) < tonumber(seq) then
        redis.call('SET', sequencer, seq)
    end
    for k, v in pairs(execV) do
        redis.call('HSET', k, 's', execS[k])
        redis.call('HSET', k, 'v', v)
        if execE[k] > 0 then redis.call('EXPIRE', k, execE[k]) else redis.call('PERSIST', k) end
    end
    -- below is same with count.lua
    -- write consumable log
    logStr = logStr .. ARGV[#KEYS * 4 + 2]
    if (#KEYS * 4 == #ARGV - 4) then
        if (ARGV[#ARGV] ~= underscore) then
            logStr = logStr .. delimiter .. ARGV[#ARGV]
        end
        redis.call('LPUSH', qPre .. ARGV[#ARGV - 1], logStr)
    end
    -- write write log
    local lastSize = tonumber(redis.call('ZCARD', writeLog .. 1))
    if (lastSize == limit) then
        for i = wShard, 1, -1  do
            if redis.call('EXISTS', writeLog .. i) == 1 then
                redis.call('RENAME', writeLog .. i, writeLog .. (i + 1))
            end
        end
    end
    redis.call('DEL', writeLog .. (wShard + 1))
    redis.call('ZADD', writeLog .. 1, seq, wLogStr)
    -- write value log for save write result
    vLogStr = string.sub(vLogStr, 1, -2)
    local lastSize = tonumber(redis.call('ZCARD', valueLog .. 1))
    if (lastSize == limit) then
        for i = wShard, 1, -1  do
            if redis.call('EXISTS', valueLog .. i) == 1 then
                redis.call('RENAME', valueLog .. i, valueLog .. (i + 1))
            end
        end
    end
    redis.call('DEL', valueLog .. (wShard + 1))
    redis.call('ZADD', valueLog .. 1, seq, seq .. at .. vLogStr)
    -- up is same with count.lua
    if (ignore ~= '') then ignore = at .. string.sub(ignore, 1, -2) end
    return reply .. ignore
else
    return argErr
end