#!/bin/bash
OTRS=/opt/otrs
WORKDIR=${OTRS}/var/tarantool/otrs_cache
TMPDIR=/tmp/otrs/tarantool

if [ ! -d ${WORKDIR} ]; then
    echo "${WORKDIR} not found!"
    if [ ! -d ${TMPDIR} ]; then
        mkdir -p ${TMPDIR}
        ln -s ${TMPDIR} ${WORKDIR}
    fi
    ln -s ${OTRS}/scripts/tarantool/stat.lua ${WORKDIR}/stat.lua
    ln -s ${OTRS}/scripts/tarantool /etc/tarantool/instances.enbled/otrs_cache
fi
echo "Done!"
