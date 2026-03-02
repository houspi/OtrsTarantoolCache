#!/bin/bash
tt stop otrs_cache
find /opt/otrs/var/tarantool/otrs_cache/wal -type f -name '*.xlog' -delete
find /opt/otrs/var/tarantool/otrs_cache/memtx -type f -name '*.snap' -delete
tt start otrs_cache