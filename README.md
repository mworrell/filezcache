[![Build Status][gh badge]][gh]
[![Hex.pm version][hexpm version]][hexpm]
[![Hex.pm Downloads][hexpm downloads]][hexpm]
[![Hex.pm Documentation][hexdocs documentation]][hexdocs]
[![Erlang Versions][erlang version badge]][gh]
[![License][license]](https://www.apache.org/licenses/LICENSE-2.0)


Filezcache - cache s3 and other files on disk
=============================================

This is a file caching system optimized for use with cowmachine
but then also be used with other services.

It has some unique properties:

 * Lookups return file references (if possible) which can be used with sendfile
 * Cached files can be streamed to requestors while they are being filled
 * On startup the cache is repopulated with existing files
 * Lookups return data formats directly useable for cowmachine serving

The system uses the file system to store files and a disk log. The files are stored
in `priv/data/` and the disk log in `priv/journal`.

The disk log is used to rebuild the cache after a start. All files are checked against
the checksum from the disk log, non-matching files are deleted from the cache.

Example
-------

    $ erl -pa ebin
    Erlang R15B03 (erts-5.9.3.1) [source] [64-bit] [smp:4:4] [async-threads:0] [kernel-poll:false]
    Eshell V5.9.3.1  (abort with ^G)
    1> application:start(crypto).
    ok
    2> application:start(filezcache).
    ok
    3> filezcache:lookup(mykey).
    {error, enoent}
    4> filezcache:insert(mykey, <<"foobar">>).
    {ok,<0.173.0>}.
    5> filezcache:lookup(mykey).
    {ok,{file,6,"priv/data/4J0I2F06043V5P0V603D4O4I6L1J5M1B4I5Y2B2C606W28131H164Z421M4X6221"}}
    6> filezcache:insert(mykey, <<>>).
    {error,{already_started,<0.173.0>}}
    7> filezcache:delete(mykey).
    ok


Configuration keys
------------------

 * `max_bytes` Maximum size of the cache in bytes, defaults to 10GiB (10737418240 bytes)
 * `journal_dir` Directory for the disk log, defaults to `priv/journal`
 * `data_dir` Directory for the cached files, defaults to `priv/data`

TODO
----

There are some known issues that need to be resolved:

 * On startup delete files that are unknown to the disk log
 * Add timeouts to `filezcache_entry` states `wait_for_data` and `streaming`
 * Extra intelligence in filezcache_entry to prevent evicting active entries during garbage collection

<!-- Badges -->
[hexpm]: https://hex.pm/packages/filezcache
[hexpm version]: https://img.shields.io/hexpm/v/filezcache.svg?style=flat-curcle "Hex version"
[hexpm downloads]: https://img.shields.io/hexpm/dt/filezcache.svg?style=flat-curcle
[hexdocs documentation]: https://img.shields.io/badge/hex-docs-purple.svg?style=flat-curcle
[hexdocs]: https://hexdocs.pm/filezcache
[gh]: https://github.com/mworrell/filezcache/actions/workflows/test.yaml
[gh badge]: https://img.shields.io/github/workflow/status/mworrell/filezcache/Test?style=flat-curcle
[erlang version badge]: https://img.shields.io/badge/Supported%20Erlang%2FOTP-22.3%20to%2024.0.1-blue.svg?style=flat-curcle
[license]: https://img.shields.io/badge/License-Apache_2.0-blue.svg?logo=apache&logoColor=red "Apache 2.0"
