#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o xtrace

SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
SVC_ROOT=/opt/smartdc/mola

source ${DIR}/scripts/util.sh
source ${DIR}/scripts/services.sh


export PATH=$SVC_ROOT/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH

function manta_add_mola_bin_to_path {
    while IFS='' read -r line
    do
        if [[ $line == export\ PATH=* ]]
        then
            B=$(echo "$line" | cut -d '=' -f 1)
            E=$(echo "$line" | cut -d '=' -f 2)
            echo $B=/opt/smartdc/mola/bin:$E
        else
            echo "$line"
        fi
    done < /root/.bashrc >/root/.bashrc_new
    mv /root/.bashrc_new /root/.bashrc
}

function manta_setup_mola {
    local crontab=/tmp/.manta_mola_cron
    crontab -l > $crontab
    [[ $? -eq 0 ]] || fatal "Unable to write to $crontab"

    #Before you change cron scheduling, please consult the Mola System "Crons"
    # Overview documentation (manta-mola.git/docs/system-crons)

    #PG Transform, Garbage Collection, Audit
    mkdir -p /opt/smartdc/common/bundle
    cd /opt/smartdc && tar -chzf /opt/smartdc/common/bundle/mola.tar.gz mola; cd -
    echo '0 2 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_pg_transform.js >>/var/log/mola-pg-transform.log 2>&1' >>$crontab
    echo '5 8 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_gc.js >>/var/log/mola.log 2>&1' >>$crontab
    echo '10 11 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/gc_create_links.js >>/var/log/mola-gc-create-links.log 2>&1' >>$crontab
    echo '15 12 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/moray_gc.js >>/var/log/mola-moray-gc.log 2>&1' >>$crontab
    echo '20 14 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_audit.js >>/var/log/mola-audit.log 2>&1' >>$crontab

    #Metering
    echo '15 08 * * * cd /opt/smartdc/mackerel && ./scripts/cron/meter-storage.sh >>/var/log/mackerel.log 2>&1' >>$crontab
    echo '15 * * * * cd /opt/smartdc/mackerel && ./scripts/cron/meter-request.sh >>/var/log/mackerel.log 2>&1' >>$crontab
    echo '15 * * * * cd /opt/smartdc/mackerel && ./scripts/cron/meter-compute.sh >>/var/log/mackerel.log 2>&1' >>$crontab
    echo '30 04 * * * cd /opt/smartdc/mackerel && ./scripts/cron/meter-previous-day.sh >>/var/log/mackerel.log 2>&1' >>$crontab
    echo '55 * * * * cd /opt/smartdc/mackerel && ./scripts/format/rep.sh' >>$crontab
    echo '55 14 * * * cd /opt/smartdc/mackerel && ./scripts/format/daily.sh' >>$crontab
    gsed -i -e "s|REDIS_HOST|$(mdata-get auth_cache_name)|g" /opt/smartdc/mackerel/etc/config.js

    crontab $crontab
    [[ $? -eq 0 ]] || fatal "Unable import crons"

    manta_add_logadm_entry "mola-pg-transform" "/var/log" "exact"
    manta_add_logadm_entry "mola" "/var/log" "exact"
    manta_add_logadm_entry "mola-gc-create-links" "/var/log" "exact"
    manta_add_logadm_entry "mola-moray-gc" "/var/log" "exact"
    manta_add_logadm_entry "mola-audit" "/var/log" "exact"
    manta_add_logadm_entry "mackerel" "/var/log" "exact"

    echo "export THOTH_USER=thoth" >> /root/.bashrc
}


# Don't use the standard rsyslog function, as this is not a forwarder
function manta_setup_rsyslogd {
    cat > /etc/rsyslog.conf <<"HERE"
$MaxMessageSize 64k

$ModLoad immark
$ModLoad imsolaris
$ModLoad imtcp
$ModLoad imudp

*.err;kern.notice;auth.notice			/dev/sysmsg
*.err;kern.debug;daemon.notice;mail.crit	/var/adm/messages

*.alert;kern.err;daemon.err			operator
*.alert						root

*.emerg						*

mail.debug					/var/log/syslog

auth.info					/var/log/auth.log
mail.info					/var/log/postfix.log

$template bunyan,"%msg:R,ERE,1,FIELD:(\{.*\})--end%\n"
$template PerHostFile,"/var/log/manta/%programname%/%$year%/%$month%/%$day%/%$hour%/%hostname%.log"
local0.* -?PerHostFile;bunyan

# Local1 is HAProxy
local1.* -?PerHostFile

$InputTCPServerRun 10514
$UDPServerRun 514

HERE

    svcadm restart system-log
    [[ $? -eq 0 ]] || fatal "Unable to restart rsyslog"

    # Note we don't want to use manta_add_logadm_entry as these logs should never
    # be uploaded, and sadly we need to manually setup log rotation as logadm
    # can't do finds. We only keep files older than a day around
    local crontab=/tmp/.manta_syslog_cron
    crontab -l > $crontab
    [[ $? -eq 0 ]] || fatal "Unable to write to $crontab"

    echo '16 * * * * /opt/local/bin/find /var/log/manta_ops -type f -mtime +2 -name "*.log" -delete' >> $crontab
    echo '17 * * * * /opt/local/bin/find /var/log/manta_ops -type d -empty -delete' >> $crontab

    crontab $crontab
    [[ $? -eq 0 ]] || fatal "Unable to import crons"


}


# Mainline

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/mackerel"
manta_add_manifest_dir "/opt/smartdc/mola"

manta_common_setup "mola"

manta_ensure_zk

echo "Setting up mola crons"
manta_setup_mola

manta_add_mola_bin_to_path

manta_setup_rsyslogd

manta_common_setup_end

echo "Applying mola-specific environment changes"
SAPI_URL="$(mdata-get SAPI_URL)"
marlin_moray_host="$(curl -s $SAPI_URL/configs/$(zonename) | \
    json metadata.MARLIN_MORAY_SHARD)"
if [[ -n "$marlin_moray_host" ]]; then
	echo "export MORAY_URL=tcp://$marlin_moray_host:2020" >> /root/.bashrc
else
	echo "warning: marlin moray URL not found"
fi

exit 0
