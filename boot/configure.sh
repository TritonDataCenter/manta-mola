#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-

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

function manta_setup_manowar_user {
    useradd -c "Man O War" -b /home -d /home/manowar -s /usr/bin/bash manowar
    usermod -K defaultpriv=basic,net_privaddr manowar
    mkdir /home/manowar
    chown manowar /home/manowar
    cp -r /root/.ssh /home/manowar/.
    chown -R manowar /home/manowar/.ssh
    cat /opt/smartdc/common/etc/config.json | \
        json -e "this.manta.sign.key='/home/manowar/.ssh/id_rsa'" \
        >/home/manowar/manta.config.json
}

function manta_add_manowar_to_path {
    while IFS='' read -r line
    do
        if [[ $line == export\ PATH=* ]]
        then
            B=$(echo "$line" | cut -d '=' -f 1)
            E=$(echo "$line" | cut -d '=' -f 2)
            echo $B=/opt/smartdc/manowar/bin:$E
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

    #Garbage Collection, Audit
    mkdir -p /opt/smartdc/common/bundle
    cd /opt/smartdc && tar -chzf /opt/smartdc/common/bundle/mola.tar.gz mola; cd -
    echo '10 * * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_gc.js >>/var/log/mola.log 2>&1' >>$crontab
    echo '2,17,32,47 * * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/gc_create_links.js >>/var/log/mola-gc-create-links.log 2>&1' >>$crontab
    echo '4,19,34,49 * * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/moray_gc.js >>/var/log/mola-moray-gc.log 2>&1' >>$crontab
    echo '21 13 * * * cd /opt/smartdc/mola && ./build/node/bin/node ./bin/kick_off_audit.js >>/var/log/mola-audit.log 2>&1' >>$crontab

    #Metering
    echo '15 * * * * cd /opt/smartdc/mackerel && ./scripts/cron/meter-previous-hour.sh >>/var/log/mackerel.log 2>&1' >>$crontab
    echo '30 2 * * * cd /opt/smartdc/mackerel && ./scripts/cron/meter-previous-day.sh >>/var/log/mackerel.log 2>&1' >>$crontab
    echo '55 * * * * cd /opt/smartdc/mackerel && ./scripts/format/rep.sh' >>$crontab
    echo '55 2 * * * cd /opt/smartdc/mackerel && ./scripts/format/daily.sh' >>$crontab
    gsed -i -e "s|REDIS_HOST|$(mdata-get auth_cache_name)|g" /opt/smartdc/mackerel/etc/config.js

    #Graphing, log crunching
    cd /opt/smartdc && tar -chzf /opt/smartdc/common/bundle/manowar.tar.gz manowar; cd -
    echo '16,46 * * * * cd /opt/smartdc/manowar && ./build/node/bin/node ./bin/kick_off_log_processing.js >>/var/log/manowar-cron.log 2>&1' >>$crontab

    #Graphing, server
    svccfg import /opt/smartdc/manowar/smf/manifests/manowar.xml \
        || fatal "unable to import manowar manifest"
    svcadm enable manowar || fatal "unable to start manowar"

    crontab $crontab
    [[ $? -eq 0 ]] || fatal "Unable import crons"

    manta_add_logadm_entry "mola" "/var/log" "exact"
    manta_add_logadm_entry "mola-gc-create-links" "/var/log" "exact"
    manta_add_logadm_entry "mola-moray-gc" "/var/log" "exact"
    manta_add_logadm_entry "mola-audit" "/var/log" "exact"
    manta_add_logadm_entry "mackerel" "/var/log" "exact"
    manta_add_logadm_entry "manowar"
    manta_add_logadm_entry "manowar-cron" "/var/log" "exact"

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

manta_setup_manowar_user

manta_add_manowar_to_path

echo "Setting up mola crons"
manta_setup_mola

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
