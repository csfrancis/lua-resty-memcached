# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_MEMCACHED_PORT} ||= 11211;

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: connect works with one invalid server
--- http_config eval: $::HttpConfig
--- config
    resolver $TEST_NGINX_RESOLVER;
    location /t {
        content_by_lua '
            local memcached = require "resty.memcached"
            local memc = memcached:new({verbose = true})

            memc:set_timeout(100) -- 100 msec

            memc:connect({
              {
                host = "127.0.0.1",
                port = $TEST_NGINX_MEMCACHED_PORT
              },
              {
                host = "www.taobao.com",
                port = $TEST_NGINX_MEMCACHED_PORT
              }
            })
            if not memc:is_connected() then
                ngx.say("failed to connect: ", err)
                return
            end

            ngx.say("connected")
        ';
    }
--- request
    GET /t
--- response_body
connected
--- error_log
lua tcp socket connect timed out

=== TEST 2: connect fails with no valid servers
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local memcached = require "resty.memcached"
            local memc = memcached:new({verbose = true})

            memc:set_timeout(100) -- 100 msec

            memc:connect({
              {
                host = "127.0.0.1",
                port = 1927
              },
              {
                host = "127.0.0.1",
                port = 1928
              }
            })
            if not memc:is_connected() then
                ngx.say("failed to connect: ", err)
                return
            end

            ngx.say("connected")
        ';
    }
--- request
    GET /t
--- response_body_like chomp
failed to connect
--- error_log
Connection refused

=== TEST 3: connect reuses continuum
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local memcached = require "resty.memcached"
            local memc = memcached:new({verbose = true})

            memc:set_timeout(100) -- 100 msec

            memc:connect({
              {
                host = "127.0.0.1",
                port = 11211
              },
              {
                host = "127.0.0.1",
                port = 11212
              }
            })
            if not memc:is_connected() then
                ngx.say("failed to connect: ", err)
                return
            end

            memc = memcached:new({verbose = true})

            memc:set_timeout(100) -- 100 msec

            local ok, err = memc:connect({
              {
                host = "127.0.0.1",
                port = 11211
              },
              {
                host = "127.0.0.1",
                port = 11212
              }
            })
            if not memc:is_connected() then
                ngx.say("failed to connect: ", err)
                return
            end

            ngx.say("connected")
        ';
    }
--- request
    GET /t
--- response_body_like chomp
connected
--- grep_error_log chop
2/2 live servers
--- grep_error_log_out
2/2 live servers

=== TEST 4: get and set use multiple servers
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local memcached = require "resty.memcached"
            local memc = memcached:new({verbose = true})

            memc:set_timeout(100) -- 100 msec

            memc:connect({
              {
                host = "127.0.0.1",
                port = 11211
              },
              {
                host = "127.0.0.1",
                port = 11212
              }
            })
            if not memc:is_connected() then
                ngx.say("failed to connect: ", err)
                return
            end

            local res, flags, ok, err
            local server1 = memcached:new()
            local server2 = memcached:new()

            server1:connect("127.0.0.1", 11211)
            server2:connect("127.0.0.1", 11212)

            if not server1:is_connected() or not server2:is_connected() then
                ngx.say("failed to connect")
                return
            end

            ngx.say("connected")

            local result = memc:flush_all()
            for _, r in ipairs(result) do
                if not r[1] then
                    ngx.say("failed to flush all: ", r[2])
                    return
                end
            end

            local k1, k2, v1, v2 = "foooooo", "foo", "one", "two"
            ok, err = memc:set(k1, v1)
            if not ok then
                ngx.say("failed to set k1: ", err)
                return
            end
            ok, err = memc:set(k2, v2)
            if not ok then
                ngx.say("failed to set k2: ", err)
                return
            end

            res, flags, err = server1:get(k1)
            if err then
                ngx.say("failed to get k1: ", err)
                return
            end
            ngx.say(k1, ":", res)

            res, flags, err = server2:get(k2)
            if err then
                ngx.say("failed to get k2: ", err)
                return
            end
            ngx.say(k2, ":", res)
        ';
    }
--- request
    GET /t
--- response_body
connected
foooooo:one
foo:two
--- no_error_log
[error]

=== TEST 5: multiget with multiple servers
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local memcached = require "resty.memcached"
            local memc = memcached:new({verbose = true})

            memc:set_timeout(100) -- 100 msec

            memc:connect({
              {
                host = "127.0.0.1",
                port = 11211
              },
              {
                host = "127.0.0.1",
                port = 11212
              }
            })
            if not memc:is_connected() then
                ngx.say("failed to connect: ", err)
                return
            end

            ngx.say("connected")

            local res, ok, err

            res = memc:flush_all()
            for _, r in ipairs(res) do
                if not r[1] then
                    ngx.say("failed to flush all: ", r[2])
                    return
                end
            end

            local k1, k2, v1, v2 = "foooooo", "foo", "one", "two"
            ok, err = memc:set(k1, v1)
            if not ok then
                ngx.say("failed to set k1: ", err)
                return
            end
            ok, err = memc:set(k2, v2)
            if not ok then
                ngx.say("failed to set k2: ", err)
                return
            end

            res, err = memc:get({k1, k2})
            if not res then
              ngx.say("failed to get keys", err)
            end

            local count = 0
            for _,_ in pairs(res) do count = count + 1 end
            ngx.say("count ", count)

            ngx.say(k1, ":", res[k1][1])
            ngx.say(k2, ":", res[k2][1])
        ';
    }
--- request
    GET /t
--- response_body
connected
count 2
foooooo:one
foo:two
--- no_error_log
[error]
