#Online Bash Shell.
#Code, Compile, Run and Debug Bash script online.
#Write your code in this editor and press "Run" button to execute it.


while true; do ping -c1 ${MYSQL_HOST} > /dev/null \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < employees.sql \
  && echo "finish load employees db" \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < sakila/sakila-mv-schema.sql \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < sakila/sakila-mv-data.sql \
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