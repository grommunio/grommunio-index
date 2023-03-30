#!/bin/bash

umask 07
if [ "$(id -u)" = 0 ]; then
	exec runuser --group groweb --supp-group gromox --shell "$0" grommunio --
	exit 1
fi

MYSQL_CFG="/etc/gromox/mysql_adaptor.cfg"

if [ ! -e "${MYSQL_CFG}" ] ; then
	echo "MySQL configuration not found. ($MYSQL_CFG)"
	exit 1
fi

MYSQL_PARAMS="--skip-column-names --skip-line-numbers"
MYSQL_USERNAME=$(sed -ne 's/^mysql_username\s*=\s*\(.*\)/\1/p' ${MYSQL_CFG})
if [ -z "$MYSQL_USERNAME" ]; then
	MYSQL_USERNAME="root"
fi
MYSQL_PASSWORD=$(sed -ne 's/^mysql_password\s*=\s*\(.*\)/\1/p' ${MYSQL_CFG})
MYSQL_DBNAME=$(sed -ne 's/^mysql_dbname\s*=\s*\(.*\)/\1/p' ${MYSQL_CFG})
if [ -z "$MYSQL_DBNAME" ]; then
	MYSQL_DBNAME="grommunio"
fi
if [ "${MYSQL_DBNAME:0:1}" = "-" ]; then
	echo "Cannot use that dbname: ${MYSQL_DBNAME}"
	exit 1
fi
MYSQL_HOST=$(sed -ne 's/^mysql_host\s*=\s*\(.*\)/-h\1/p' ${MYSQL_CFG})
if [ -z "$MYSQL_HOST" ]; then
	MYSQL_HOST="localhost"
fi
MYSQL_QUERY='SELECT u.username, u.maildir, COALESCE(s.hostname, "::1") AS hostname FROM users u LEFT JOIN servers s ON u.homeserver = s.id OR u.homeserver IS NULL AND s.id = 1 WHERE u.id <> 0 AND u.maildir <> "";'

CONFIG_FILE=$(mktemp)
cat <<CONFFILE >"${CONFIG_FILE}"
[client]
user=${MYSQL_USERNAME}
password=${MYSQL_PASSWORD}
host=${MYSQL_HOST}
database=${MYSQL_DBNAME}
CONFFILE

MYSQL_CMD="mysql --defaults-file=${CONFIG_FILE} ${MYSQL_PARAMS}"
WEB_INDEX_PATH="/var/lib/grommunio-web/sqlite-index"

echo "${MYSQL_QUERY[@]}" | ${MYSQL_CMD} | while read -r USERNAME MAILDIR HOST ; do
	if [ "${USERNAME:0:1}" = "-" ]; then
		exit 1
	fi
	if [ -n "${MAILDIR}" ] && [ -d "${MAILDIR}" ] && [ -n "${HOST}" ] && [ -d "${WEB_INDEX_PATH}/${USERNAME}" ] || mkdir -p "${WEB_INDEX_PATH}/${USERNAME}/"; then
		if [ -v ADDITIONAL_PARAM ]; then
			grommunio-index "${ADDITIONAL_PARAM}" "${MAILDIR}" -e "${HOST}" -o "${WEB_INDEX_PATH}/${USERNAME}/index.sqlite3"
		else
			grommunio-index "${MAILDIR}" -e "${HOST}" -o "${WEB_INDEX_PATH}/${USERNAME}/index.sqlite3"
		fi
	fi
done

rm -f "${CONFIG_FILE}"
