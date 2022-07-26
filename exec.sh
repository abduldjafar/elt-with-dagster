while true; do ping -c1 mysql  > /dev/null &&  break ; done
echo "mysql connected in dagster container"

while true; do ping -c1 ${MYSQL_HOST} > /dev/null \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < test_db-master/employees.sql \
  && echo "finish load employees db" \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < test_db-master/sakila/sakila-mv-schema.sql \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < test_db-master/sakila/sakila-mv-data.sql \
  && echo "finish load sakila db" \
  && break; done 

while true; do ping -c1 ${CH_HOST} > /dev/null && clickhouse-client -h ${CH_HOST} -q "create database dwh" \
    && echo "finish create dwh db in clickhouse server" \
    && break; done

while true; do ping -c1 ${MONGO_HOST} > /dev/null \
    && echo "mongodb up" \
    && break; done

cd ../mongodb-sample-dataset-main
chmod 777 script.sh
./script.sh ${MONGO_HOST} ${MONGO_PORT} ${MONGO_USER} ${MONGO_PASSWORD}
echo "finis load mongodb datas"

cd /opt/dag

dagit  -h  0.0.0.0 -p 3000