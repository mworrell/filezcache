Filecache - webzmachine companion
=================================

This is a file caching system optimized for use with webzmachine.

It has some unique properties:

 * Lookups return data formats directly useable for webzmachine serving
 * Lookups return file references (if possible) which can be used with sendfile
 * Cached files can be streamed to (webzmachine) requestors while they are being filled
 * On startup the cache is repopulated with existing files

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
    2> application:start(filecache).
    ok
    3> filecache:lookup(mykey).
    {error, not_found}
    4> filecache:insert(mykey, <<"foobar">>).
    {ok,<0.173.0>}.
    5> filecache:lookup(mykey).
    {ok,{filename,6,
                  "priv/data/4J0I2F06043V5P0V603D4O4I6L1J5M1B4I5Y2B2C606W28131H164Z421M4X6221"}}
    6> filecache:insert(mykey, <<>>).
    {error,{already_started,<0.173.0>}}
    7> filecache:deleye(mykey).
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
 * Add timeouts to `filecache_entry` states `wait_for_data` and `streaming`
 * Extra intelligence in filecache_entry to prevent evicting active entries during garbage collection
