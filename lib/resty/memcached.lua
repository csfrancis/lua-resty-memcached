-- Copyright (C) 2012-2013 Yichun Zhang (agentzh), CloudFlare Inc.


local sub = string.sub
local escape_uri = ngx.escape_uri
local unescape_uri = ngx.unescape_uri
local match = string.match
local tcp = ngx.socket.tcp
local strlen = string.len
local concat = table.concat
local setmetatable = setmetatable
local type = type
local error = error
local bit, ffi

local _M = {
    _VERSION = '0.12'
}

local _continuum = {}

local MEMCACHED_DEFAULT_PORT = 11211
local MEMCACHED_DEFAULT_RETRY_TIME = 30
local MEMCACHED_DEFAULT_WEIGHT = 8
local MEMCACHED_POINTS_PER_SERVER_KETAMA = 160

local FNV_32_INIT = 2166136261
local FNV_32_PRIME = 16777619

local mt = { __index = _M }

local function _each_server(self, func)
    local last_err
    for _,server in ipairs(self.servers) do
        local _, err = func(server)
        if err then last_err = err end
    end
    return last_err
end

local function _each_socket(self, func)
    return _each_server(self, function(server)
        return func(server.sock)
    end)
end

local function _any(t, func)
    for _,v in ipairs(t) do
        if func(v) then return true end
    end
    return false
end

local function _has_single_element(t)
    local count = 0
    for _ in pairs(t) do
        count = count + 1
        if count > 1 then return false end
    end
    return count == 1 or false
end

local function _server_connected(server)
    if not server.sock then return false end
    return true
end

local function _handle_server_error(self, server, update_continuum, err)
    if self.verbose then
        print(string.format("error on %s:%d %s", server.host, server.port, tostring(err)))
    end
    server.sock = nil
    if update_continuum then
        _update_continuum(self)
    end
end

local function _create_server(host, port, opts)
    local server = {}
    server.host = host
    server.port = port
    server.opts = opts or {}
    server.opts.weight = server.opts.weight or MEMCACHED_DEFAULT_WEIGHT
    return server
end

local function _connect_server(self, server)
    local ok, err
    server.sock = tcp()
    if not server.sock then
        return nil, err
    end

    if self.timeout then _M.set_timeout(self, self.timeout) end

    ok, err = server.sock:connect(server.host, server.port, server.opts)
    if not ok then
        _handle_server_error(self, server, false, err)
    end
    return ok, err
end

local function _fnv1_32(key)
    local hash = FNV_32_INIT
    for i=1, key:len() do
        hash = tonumber(ffi.cast('uint32_t', ffi.cast('uint32_t', hash) * ffi.cast('uint32_t', FNV_32_PRIME)))
        hash = bit.bxor(hash, key:byte(i))
    end
    return hash
end

local function _hash_key(key)
    return _fnv1_32(key)
end

local function _find_server_index(continuum, hash)
    local first, last, middle = 1
    local left = first
    local right = last

    while left < right do
        middle = math.floor(left + (right - left) / 2)
        if continuum[middle].value < hash then
            left = middle + 1
        else
            right = middle
        end
    end

    if right == last then
        right = first
    end

    return continuum[right].server_index
end

local function _add_server_to_continuum(continuum, server, server_index, total_weight, live_servers)
    local pct = server.opts.weight / total_weight
    local pointer_per_server = (math.floor(pct * MEMCACHED_POINTS_PER_SERVER_KETAMA / 4 * live_servers + 0.0000000001) * 4)
    local pointer_per_hash = 4

    for pointer_index = 1, pointer_per_server / pointer_per_hash do
        local sort_host
        if server.port == MEMCACHED_DEFAULT_PORT then
            sort_host = string.format("%s-%d", server.host, pointer_index - 1)
        else
            sort_host = string.format("%s:%d-%d", server.host, server.port, pointer_index - 1)
        end

        local result = ngx.md5_bin(sort_host)
        for i=1, pointer_per_hash do
            local alignment = i - 1
            local value = bit.bor(bit.lshift(bit.band(result:byte(4 + alignment * 4), 0xff), 24),
                bit.lshift(bit.band(result:byte(3 + alignment * 4), 0xff), 16),
                bit.lshift(bit.band(result:byte(2 + alignment * 4), 0xff), 8),
                bit.band(result:byte(1 + alignment * 4), 0xff))
            entry = {}
            entry.server_index = server_index
            entry.value = value
            table.insert(continuum, entry)
        end
    end
end

local function _get_continuum_hash(self, pred)
    local t = {}
    _each_server(self, function(server)
        if not pred or pred(server) then
            table.insert(t, server.host)
            table.insert(t, server.port)
            table.insert(t, server.weight)
        end
    end)
    return ngx.md5(table.concat(t, ","))
end

local function _get_live_server_continuum_hash(self)
    return _get_continuum_hash(self, function(server) return server.sock end)
end

local function _build_continuum(self)
    local total_weight = 0
    local live_servers = 0
    local continuum = {}
    for _,server in ipairs(self.servers) do
        if _server_connected(server) then
            live_servers = live_servers + 1
            total_weight = total_weight + server.opts.weight
        end
    end

    for idx,server in ipairs(self.servers) do
        if _server_connected(server) then
            _add_server_to_continuum(continuum, server, idx, total_weight, live_servers)
        end
    end

    table.sort(continuum, function(a, b) return a.value < b.value end)

    if self.verbose then
        print(string.format("built continuum with %d entries for %d/%d live servers",
            #continuum, live_servers, #self.servers))
    end

    return continuum
end

local function _update_continuum(self)
    local c = _continuum[self.continuum_hash]
    if not c then
        c = {
            rebuilding = false,
            build_time = 0
        }
        _continuum[self.continuum_hash] = c
    end

    if c.rebuilding or c.build_time > self.connect_time then
        return
    end

    c.rebuilding = true
    c.data = _build_continuum(self)
    c.hash = _get_live_server_continuum_hash(self)
    c.build_time = ngx.now()
    c.rebuilding = false

    return c.data
end

local function _get_sock(self, key)
    if #self.servers == 1 then
        return self.servers[1].sock
    elseif #self.servers > 1 then
        idx = _find_server_index(self.continuum, _hash_key(key))
        return self.servers[idx] and self.servers[idx].sock or nil
    else
        return nil
    end
end

function _M.new(self, opts)
    local escape_key = escape_uri
    local unescape_key = unescape_uri
    local verbose = false

    if opts then
       local key_transform = opts.key_transform

       if key_transform then
          escape_key = key_transform[1]
          unescape_key = key_transform[2]
          if not escape_key or not unescape_key then
             return nil, "expecting key_transform = { escape, unescape } table"
          end
       end

       if opts.verbose then
           verbose = opts.verbose
       end
    end

    return setmetatable({
        servers = {},
        connect_time = ngx.now(),
        escape_key = escape_key,
        unescape_key = unescape_key,
        verbose = verbose
    }, mt)
end


function _M.set_timeout(self, timeout)
    self.timeout = timeout
    if #self.servers > 0 then
        _each_socket(self, function(sock)
            if sock then
                sock:settimeout(timeout)
            end
        end)
    end
end


function _M.connect(self, ...)
    local arg = {...}

    for i in ipairs(self.servers) do self.servers[i] = nil end

    if type(arg[1]) == "table" then
        if not bit then
            bit = require("bit")
            ffi = require("ffi")
        end
        for _, s in ipairs(arg[1]) do
            table.insert(self.servers, _create_server(s.host, s.port, s.opts))
        end
    else
        table.insert(self.servers, _create_server(unpack(arg)))
    end

    local err = _each_server(self, function(server)
        return _connect_server(self, server)
    end)

    if #self.servers > 1 then
        self.continuum_hash = _get_continuum_hash(self)
        local c = _continuum[self.continuum_hash]
        self.continuum = c and c.data or nil
        if not c or c.hash ~= _get_live_server_continuum_hash(self) then
            self.continuum = _update_continuum(self)
        end
    end

    -- return true so long as we're connected to one server
    if err and _any(self.servers, _server_connected) then
        return 1, nil
    end

    return (not err and 1 or nil), err
end


local function _multi_get(self, keys)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local nkeys = #keys

    if nkeys == 0 then
        return {}, nil
    end

    local escape_key = self.escape_key
    local cmd = {"get"}
    local n = 1

    for i = 1, nkeys do
        cmd[n + 1] = " "
        cmd[n + 2] = escape_key(keys[i])
        n = n + 2
    end
    cmd[n + 1] = "\r\n"

    -- print("multi get cmd: ", cmd)

    local bytes, err = sock:send(concat(cmd))
    if not bytes then
        return nil, err
    end

    local unescape_key = self.unescape_key
    local results = {}

    while true do
        local line, err = sock:receive()
        if not line then
            return nil, err
        end

        if line == 'END' then
            break
        end

        local key, flags, len = match(line, '^VALUE (%S+) (%d+) (%d+)$')
        -- print("key: ", key, "len: ", len, ", flags: ", flags)

        if key then

            local data, err = sock:receive(len)
            if not data then
                return nil, err
            end

            results[unescape_key(key)] = {data, flags}

            data, err = sock:receive(2) -- discard the trailing CRLF
            if not data then
                return nil, err
            end
        end
    end

    return results
end


function _M.get(self, key)
    if type(key) == "table" then
        return _multi_get(self, key)
    end

    local sock = _get_sock(self)
    if not sock then
        return nil, nil, "not initialized"
    end

    local bytes, err = sock:send("get " .. self.escape_key(key) .. "\r\n")
    if not bytes then
        return nil, nil, "failed to send command: " .. (err or "")
    end

    local line, err = sock:receive()
    if not line then
        return nil, nil, "failed to receive 1st line: " .. (err or "")
    end

    if line == 'END' then
        return nil, nil, nil
    end

    local flags, len = match(line, '^VALUE %S+ (%d+) (%d+)$')
    if not flags then
        return nil, nil, "bad line: " .. line
    end

    -- print("len: ", len, ", flags: ", flags)

    local data, err = sock:receive(len)
    if not data then
        return nil, nil, "failed to receive data chunk: " .. (err or "")
    end

    line, err = sock:receive(2) -- discard the trailing CRLF
    if not line then
        return nil, nil, "failed to receive CRLF: " .. (err or "")
    end

    line, err = sock:receive() -- discard "END\r\n"
    if not line then
        return nil, nil, "failed to receive END CRLF: " .. (err or "")
    end

    return data, flags
end


local function _multi_gets(self, keys)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local nkeys = #keys

    if nkeys == 0 then
        return {}, nil
    end

    local escape_key = self.escape_key
    local cmd = {"gets"}
    local n = 1
    for i = 1, nkeys do
        cmd[n + 1] = " "
        cmd[n + 2] = escape_key(keys[i])
        n = n + 2
    end
    cmd[n + 1] = "\r\n"

    -- print("multi get cmd: ", cmd)

    local bytes, err = sock:send(concat(cmd))
    if not bytes then
        return nil, err
    end

    local unescape_key = self.unescape_key
    local results = {}

    while true do
        local line, err = sock:receive()
        if not line then
            return nil, err
        end

        if line == 'END' then
            break
        end

        local key, flags, len, cas_uniq =
                match(line, '^VALUE (%S+) (%d+) (%d+) (%d+)$')

        -- print("key: ", key, "len: ", len, ", flags: ", flags)

        if key then

            local data, err = sock:receive(len)
            if not data then
                return nil, err
            end

            results[unescape_key(key)] = {data, flags, cas_uniq}

            data, err = sock:receive(2) -- discard the trailing CRLF
            if not data then
                return nil, err
            end
        end
    end

    return results
end


function _M.gets(self, key)
    if type(key) == "table" then
        return _multi_gets(self, key)
    end

    local sock = _get_sock(self)
    if not sock then
        return nil, nil, nil, "not initialized"
    end

    local bytes, err = sock:send("gets " .. self.escape_key(key) .. "\r\n")
    if not bytes then
        return nil, nil, err
    end

    local line, err = sock:receive()
    if not line then
        return nil, nil, nil, err
    end

    if line == 'END' then
        return nil, nil, nil, nil
    end

    local flags, len, cas_uniq = match(line, '^VALUE %S+ (%d+) (%d+) (%d+)$')
    if not flags then
        return nil, nil, nil, line
    end

    -- print("len: ", len, ", flags: ", flags)

    local data, err = sock:receive(len)
    if not data then
        return nil, nil, nil, err
    end

    line, err = sock:receive(2) -- discard the trailing CRLF
    if not line then
        return nil, nil, nil, err
    end

    line, err = sock:receive() -- discard "END\r\n"
    if not line then
        return nil, nil, nil, err
    end

    return data, flags, cas_uniq
end


local function _expand_table(value)
    local segs = {}
    local nelems = #value
    local nsegs = 0
    for i = 1, nelems do
        local seg = value[i]
        nsegs = nsegs + 1
        if type(seg) == "table" then
            segs[nsegs] = _expand_table(seg)
        else
            segs[nsegs] = seg
        end
    end
    return concat(segs)
end


local function _store(self, cmd, key, value, exptime, flags)
    if not exptime then
        exptime = 0
    end

    if not flags then
        flags = 0
    end

    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    if type(value) == "table" then
        value = _expand_table(value)
    end

    local req = cmd .. " " .. self.escape_key(key) .. " " .. flags .. " "
                .. exptime .. " " .. strlen(value) .. "\r\n" .. value
                .. "\r\n"
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    local data, err = sock:receive()
    if not data then
        return nil, err
    end

    if data == "STORED" then
        return 1
    end

    return nil, data
end


function _M.set(self, ...)
    return _store(self, "set", ...)
end


function _M.add(self, ...)
    return _store(self, "add", ...)
end


function _M.replace(self, ...)
    return _store(self, "replace", ...)
end


function _M.append(self, ...)
    return _store(self, "append", ...)
end


function _M.prepend(self, ...)
    return _store(self, "prepend", ...)
end


function _M.cas(self, key, value, cas_uniq, exptime, flags)
    if not exptime then
        exptime = 0
    end

    if not flags then
        flags = 0
    end

    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local req = "cas " .. self.escape_key(key) .. " " .. flags .. " "
                .. exptime .. " " .. strlen(value) .. " " .. cas_uniq
                .. "\r\n" .. value .. "\r\n"

    -- local cjson = require "cjson"
    -- print("request: ", cjson.encode(req))

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    local line, err = sock:receive()
    if not line then
        return nil, err
    end

    -- print("response: [", line, "]")

    if line == "STORED" then
        return 1
    end

    return nil, line
end


function _M.delete(self, key)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    key = self.escape_key(key)

    local req = "delete " .. key .. "\r\n"

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    local res, err = sock:receive()
    if not res then
        return nil, err
    end

    if res ~= 'DELETED' then
        return nil, res
    end

    return 1
end


function _M.set_keepalive(self, ...)
    arg = {...}
    local err = _each_socket(self, function(sock)
        if not sock then
            return nil, "not initialized"
        end
        return sock:setkeepalive(unpack(arg))
    end)
    return (not err and 1 or nil), err
end


function _M.get_reused_times(self)
    count = 0
    local err = _each_socket(self, function(sock)
        local sock = _get_sock(self)
        if not sock then
            return nil, "not initialized"
        end

        local c, err = sock:getreusedtimes()
        count = count + (c and c or 0)
        return count, err
    end)
    return (not err and count or nil), err
end


function _M.flush_all(self, time)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local req
    if time then
        req = "flush_all " .. time .. "\r\n"
    else
        req = "flush_all\r\n"
    end

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    local res, err = sock:receive()
    if not res then
        return nil, err
    end

    if res ~= 'OK' then
        return nil, res
    end

    return 1
end


local function _incr_decr(self, cmd, key, value)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local req = cmd .. " " .. self.escape_key(key) .. " " .. value .. "\r\n"

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    local line, err = sock:receive()
    if not line then
        return nil, err
    end

    if not match(line, '^%d+$') then
        return nil, line
    end

    return line
end


function _M.incr(self, key, value)
    return _incr_decr(self, "incr", key, value)
end


function _M.decr(self, key, value)
    return _incr_decr(self, "decr", key, value)
end


function _M.stats(self, args)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local req
    if args then
        req = "stats " .. args .. "\r\n"
    else
        req = "stats\r\n"
    end

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    local lines = {}
    local n = 0
    while true do
        local line, err = sock:receive()
        if not line then
            return nil, err
        end

        if line == 'END' then
            return lines, nil
        end

        if not match(line, "ERROR") then
            n = n + 1
            lines[n] = line
        else
            return nil, line
        end
    end

    -- cannot reach here...
    return lines
end


function _M.version(self)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send("version\r\n")
    if not bytes then
        return nil, err
    end

    local line, err = sock:receive()
    if not line then
        return nil, err
    end

    local ver = match(line, "^VERSION (.+)$")
    if not ver then
        return nil, ver
    end

    return ver
end


function _M.quit(self)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send("quit\r\n")
    if not bytes then
        return nil, err
    end

    return 1
end


function _M.verbosity(self, level)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send("verbosity " .. level .. "\r\n")
    if not bytes then
        return nil, err
    end

    local line, err = sock:receive()
    if not line then
        return nil, err
    end

    if line ~= 'OK' then
        return nil, line
    end

    return 1
end


function _M.touch(self, key, exptime)
    local sock = _get_sock(self)
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send("touch " .. self.escape_key(key) .. " "
                                 .. exptime .. "\r\n")
    if not bytes then
        return nil, err
    end

    local line, err = sock:receive()
    if not line then
        return nil, err
    end

    -- moxi server from couchbase returned stored after touching
    if line == "TOUCHED" or line =="STORED" then
        return 1
    end
    return nil, line
end


function _M.close(self)
    local err = _each_socket(self, function(sock)
        if not sock then
            return nil, "not initialized"
        end
        return sock:close()
    end)
    return (not err and 1 or nil), err
end


return _M
