-- ./redis-cli -p 6379 --eval count.lua k1 k2 , v1 lowBound_1 upBound_1 expire_1 v2 lowBound_2 upBound_2 expire_2 id timestamp qShardIndex customLog
-- lowBound, upBound could be '_' means default.
-- expire could be '_' means persist.
-- qShardIndex, customLog is option, if provided and customLog could be '_' means ignore.
-- response type is string, format is ok@currentValue_1,seq_1;currentValue_2,seq_2
-- if repeat the repsonse format is repeat exec@currentValue_1;currentValue_2
local function split(str, delimiter)
    if str==nil or str=='' or delimiter==nil then
        return nil
    end
    local result = {}
    for match in (str..delimiter):gmatch('(.-)'..delimiter) do
        table.insert(result, match)
    end
    return result
end
if (#KEYS > 0 and #KEYS * 4 == #ARGV - 4) or (#KEYS > 0 and #KEYS * 4 == #ARGV - 2) then
    local execV = {}; local execE = {}; local underscore = '_'
    local logStr = ''; local wLogStr = ''; local vLogStr = ''
    for i = 1, #KEYS do
        local idx = 4 * i - 3
        local k = KEYS[i]
        local v = tonumber(ARGV[idx])
        local l = ARGV[idx + 1]
        local h = ARGV[idx + 2]
        local e = ARGV[idx + 3]
        if (l == underscore) then l = lowBound else l = tonumber(l) end
        if (h == underscore) then h = upBound else h = tonumber(h) end
        if (e == underscore) then e = 0 else e = tonumber(e) end
        local old = redis.call('HGET', k, 'v')
        if old == false then
            return noKey .. k
        elseif tonumber(old) + v < l then
            return insufficient .. k
        elseif tonumber(old) + v > h then
            return overflow .. k
        end
        execV[k] = v; execE[k] = e
        wLogStr = wLogStr .. k .. ':' .. v .. ';'
    end
    -- build write log & check whether executed
    wLogStr = ARGV[#KEYS * 4 + 1] .. at .. wLogStr .. ARGV[#KEYS * 4 + 2]
    if (#KEYS * 4 == #ARGV - 4 and ARGV[#ARGV] ~= underscore) then
        wLogStr = wLogStr .. delimiter .. ARGV[#ARGV]
    end
    for i = 1, wShard do
        local score = redis.call('ZSCORE', writeLog .. i, wLogStr)
        if score ~= false then
            local vs = redis.call('ZRANGEBYSCORE', valueLog .. i, score, score)
            -- print(split(vs[1], at)[2])
            return repeatExec .. at .. split(vs[1], at)[2]
        end
    end
    -- update k/v & build reply
    local seq = redis.call('INCR', sequencer)
    for i = 1, #KEYS do
        local k = KEYS[i]; local v = execV[k]
        redis.call('HSET', k, 's', seq)
        local now = redis.call('HINCRBY', k, 'v', v)
        if execE[k] > 0 then redis.call('EXPIRE', k, execE[k]) else redis.call('PERSIST', k) end
        logStr = logStr .. k .. ':' .. v .. ',' .. now .. ';'
        vLogStr = vLogStr .. now .. ';'
        reply = reply .. now .. ',' .. seq .. ';'
    end
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
    -- trim reply
    if (string.len(reply) > 0) then reply = string.sub(reply, 1, -2) end
    return reply
else
    return argErr
end