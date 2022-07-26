FROM python
ENV MYSQL_HOST=value
ENV MYSQL_USER=value
ENV MYSQL_PASSWORD=value
ENV MYSQL_PORT=value
ENV CH_HOST=value
ENV MONGO_HOST=value
ENV MONGO_PORT=value
ENV MONGO_USER=value
ENV MONGO_PASSWORD=value
ENV MONGO_PORT=value

COPY req.txt req.txt
RUN apt update -y && apt install iputils-ping -y \
    && pip install  mariadb==1.0.11 \
    && pip install -r req.txt && pip install "pymongo[srv]" dbt-core dbt-clickhouse

RUN apt-get update -y && apt install gnupg2  wget unzip mariadb-client  iputils-ping -y \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 \
    && echo "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/" | tee /etc/apt/sources.list.d/clickhouse.list

RUN apt-get update -y && apt install   clickhouse-client -y \
    && wget https://codeload.github.com/datacharmer/test_db/zip/refs/heads/master \
    && wget https://codeload.github.com/neelabalan/mongodb-sample-dataset/zip/refs/heads/main --output-document=mongodb-sample-dataset.zip \
    && wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2004-x86_64-100.5.3.deb --output-document=mongodb-database-tools.deb\
    && unzip master && unzip mongodb-sample-dataset.zip && apt install ./mongodb-database-tools.deb -y\
    && wget https://downloads.mysql.com/docs/sakila-db.zip --output-document=sakila-db.zip && unzip sakila-db.zip \
    && cp sakila-db/* test_db-master/

COPY pipelines /opt/dag
WORKDIR /opt/dag
EXPOSE 3000
COPY exec.sh exec.sh
COPY profiles.yml pipelines/dbt_project

ENTRYPOINT [ "bash","exec.sh" ]
