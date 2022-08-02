from servers.clickhouse_server import ClickhouseServer
from servers.mongodb import MongoServer
from dagster import op, Out
import json

mongodb = MongoServer()
ch_server = ClickhouseServer()
ch_server.init()
mongodb.init()


def mongo_full_loads(
    name="default_name",
    db_name="default",
    collection_name="default_tb_name",
    db_destination="default_db_destination",
):
    @op(name=name, out={name: Out()})
    def full_loads_from_collection():

        # Op logic here
        mongodb.choose_db(db_name)
        mongodb.choose_collection(collection_name)

        datas = [(json.dumps(data, default=str),) for data in mongodb.get_all_datas()]
        return (datas, collection_name, db_destination)

    return full_loads_from_collection


def clickhouse_insert_datas(name="default", datas=()):
    @op(name=name, out={name: Out()})
    def insert_datas_to_clickhouse(datas):
        clickhouse_datas = datas[0]
        collection_name = datas[1]
        db_name = datas[2]

        clickhouse_columns = "datas String"
        order_by = "datas"
        collection_name = "{}_{}".format("mongo", collection_name)

        ch_server.create_table_with_columns(
            collection_name, clickhouse_columns, db_name, order_by
        )
        ch_server.insert_data(
            db_name,
            collection_name,
            "({})".format("datas"),
            clickhouse_datas,
        )

        return "success"

    return insert_datas_to_clickhouse
