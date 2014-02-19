Filecache - cache webzmachine companion
=======================================

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
