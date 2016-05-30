-- ./redis-cli -p 6379 --eval exist.lua xxx
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
if #KEYS == 1 then
    for i = 1, wShard do
        local score = redis.call('ZSCORE', writeLog .. i, KEYS[1])
        if score ~= false then
            local vs = redis.call('ZRANGEBYSCORE', valueLog .. i, score, score)
            -- print(split(vs[1], at)[2])
            return '1' .. at .. split(vs[1], at)[2]
        end
    end
    return '0'
else
 return arrErr
end