docker compose down
docker stop $(Docker ps -a | grep dagster | awk '{ print $1}')
docker rm -f $(Docker ps -a | grep dagster | awk '{ print $1}')
docker rmi -f $(docker images | grep dagster | awk '{ print $3}')
docker compose up