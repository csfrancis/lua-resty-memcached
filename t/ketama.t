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

            local ok, err = memc:connect({
              {
                host = "127.0.0.1",
                port = $TEST_NGINX_MEMCACHED_PORT
              },
              {
                host = "www.taobao.com",
                port = $TEST_NGINX_MEMCACHED_PORT
              }
            })
            if not ok then
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

            local ok, err = memc:connect({
              {
                host = "127.0.0.1",
                port = 1927
              },
              {
                host = "127.0.0.1",
                port = 1928
              }
            })
            if not ok then
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
            if not ok then
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
            if not ok then
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
