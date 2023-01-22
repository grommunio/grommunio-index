#!/bin/bash
if ! test $USER = "groweb"; then echo "Please run ${BASH_SOURCE[0]} as user 'groweb' or use the preferred way 'systemctl start grommunio-index.service' instead."; exit 1; fi
MYSQL_CFG="/etc/gromox/mysql_adaptor.cfg"

if [ ! -r "${MYSQL_CFG}" ] ; then
  echo "MySQL configuration not found or readable by $USER. ($MYSQL_CFG)"
  exit 1
fi

mysql_params="--skip-column-names --skip-line-numbers"
mysql_username=$(sed -ne 's/mysql_username\s*=\s*\(.*\)/-u\1/p' ${MYSQL_CFG})
mysql_password=$(sed -ne 's/mysql_password\s*=\s*\(.*\)/-p\1/p' ${MYSQL_CFG})
mysql_dbname=$(sed -ne 's/mysql_dbname\s*=\s*\(.*\)/\1/p' ${MYSQL_CFG})
mysql_host=$(sed -ne 's/mysql_host\s*=\s*\(.*\)/-h\1/p' ${MYSQL_CFG})
mysql_port=$(sed -ne 's/mysql_port\s*=\s*\(.*\)/-P\1/p' ${MYSQL_CFG})
mysql_query='select username, maildir from users where id <> 0 and maildir <> "";'
mysql_cmd="mysql ${mysql_params} ${mysql_username} ${mysql_password} ${mysql_host} ${mysql_port} ${mysql_dbname}"
web_index_path="/var/lib/grommunio-web/sqlite-index"

echo "${mysql_query}" | ${mysql_cmd} | while read -r username maildir ; do
  [ -e "${web_index_path}/${username}/" ] || mkdir "${web_index_path}/${username}/"
  grommunio-index "$maildir" -o "$web_index_path/$username/index.sqlite3"
done
