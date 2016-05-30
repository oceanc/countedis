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
local function mod(s, divisor)
    local nan = divisor
    local is = ''
    for i = 1, string.len(s) do
        local b = string.byte(s, i)
        if b >= 48 and b <= 57 then is = is .. string.char(b) else nan = nan + b end
    end
    if (string.len(is) > 15) then is = string.sub(is, -15, -1) end
    local digit = tonumber(is) + nan
    return digit % divisor
end
-- not consider expire
local function inverseWrite(wlog, vlog, curSeq)
    local xx = split(wlog, at)[2]
    local yy = split(vlog, at)[2]
    local aa = split(xx, delimiter)
    local bizLog = ''; local inverseKV = {}; local qIdx = {}
    local kvs = split(aa[1], ';')
    local vs = split(yy, ';')
    local time = kvs[#kvs]
    for i = 1, #kvs - 1 do
        local kv = split(kvs[i], ':')
        local k = kv[1]
        local a = kv[2]
        local v = vs[i]
        inverseKV[k] = -tonumber(a)
        qIdx[i] = mod(k, qShard)
        bizLog = bizLog .. k .. ':' .. a .. ',' .. v .. ';'
    end
    bizLog = bizLog .. time
    for k, v in pairs(inverseKV) do
        redis.call('HINCRBY', k, 'v', v)
        redis.call('HSET', k, 's', curSeq)
    end

    if #aa == 2 then bizLog = bizLog .. delimiter .. aa[2] end
    local q = qPre .. qIdx[1]
    redis.call('LREM', q, 1, bizLog)
    return true
end
if #KEYS ~= 1 then
    return arrErr
else
    local curSeq = tonumber(KEYS[1]); local maxIdx = 0; local maxDelIdx = 0
    for i = 1, wShard do
        if redis.call('EXISTS', writeLog .. i) == 1 then maxIdx = i end
    end
    local lastSize = tonumber(redis.call('ZCOUNT', writeLog .. maxIdx, '(' .. curSeq, '+inf'))
    if lastSize >= limit then
        return rollbackInsufficient
    end

    for i = 1, wShard do
        local wLogs = redis.call('ZRANGEBYSCORE', writeLog .. i, '(' .. curSeq, '+inf')
        local vLogs = redis.call('ZRANGEBYSCORE', valueLog .. i, '(' .. curSeq, '+inf')
        for i = 1, #wLogs do
            if inverseWrite(wLogs[i], vLogs[i], curSeq) == false then return rollbackInsufficient end
        end
        if #wLogs == limit then
            redis.call('DEL', writeLog .. i)
            redis.call('DEL', valueLog .. i)
            maxDelIdx = i
        else
            redis.call('ZREMRANGEBYSCORE', writeLog .. i, '(' .. curSeq, '+inf')
            redis.call('ZREMRANGEBYSCORE', valueLog .. i, '(' .. curSeq, '+inf')
        end
    end
    if maxDelIdx > 0 then
        for i = maxDelIdx + 1, maxIdx do
            redis.call('RENAME', writeLog .. i, writeLog .. (i - 1))
            redis.call('RENAME', valueLog .. i, valueLog .. (i - 1))
        end
    end

    redis.call('SET', sequencer, curSeq)
    return reply
end