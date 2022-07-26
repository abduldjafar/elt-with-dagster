from servers.clickhouse_server import ClickhouseServer

class ClickhouseServices(object):
    def __init__(self):
        pass

    def insert_general_datas_to_clickhouse(self,tb_name,raw_columns,clickhouse_columns,db_name,order_by,datas,ch_server=ClickhouseServer()):
        ch_server.create_table_with_columns(
                tb_name, clickhouse_columns, db_name, order_by
            )
        
        ch_server.insert_data(
                db_name,
                tb_name,
                "({})".format(
                    ",".join(
                        raw_columns
                    )
                ),
                datas,
            )

        return "success"

    def insert_datas_from_mysqldatas_to_clickhouse(self,name="default", datas=(), db_sources="", ch_server=ClickhouseServer()):
            mysql_datas = datas[0]
            tb_name = datas[1]
            db_name = datas[2]

            count_null_columns = sum(
                [
                    1 if columns[2] == "YES" else 0
                    for columns in mysql_datas["table_details"]
                ]
            )

            if count_null_columns == len(mysql_datas["table_details"]):
                clickhouse_columns = ",".join(
                    [
                        "{} {}".format(
                            "_".join(columns[0].split(" ")),
                            ch_server.convert_data_type_from_mysql()[columns[1].upper()],
                        )
                        for columns in mysql_datas["table_details"]
                    ]
                )
            else:
                clickhouse_columns = ",".join(
                    [
                        "{} Nullable({})".format(
                            "_".join(columns[0].split(" ")),
                            ch_server.convert_data_type_from_mysql()[columns[1].upper()],
                        )
                        if columns[2] == "YES"
                        else "{} {}".format(
                            "_".join(columns[0].split(" ")),
                            ch_server.convert_data_type_from_mysql()[columns[1].upper()],
                        )
                        for columns in mysql_datas["table_details"]
                    ]
                )

            columns_for_order = list(
                filter(lambda x: x[2] == "NO", mysql_datas["table_details"])
            )
            order_by = (
                columns_for_order[0][0]
                if len(columns_for_order) > 0
                else mysql_datas["table_details"][0][0]
            )

            tb_name = "{}_{}".format(db_sources, tb_name)

            ch_server.create_table_with_columns(
                tb_name, clickhouse_columns, db_name, order_by
            )
            ch_server.insert_data(
                db_name,
                tb_name,
                "({})".format(
                    ",".join(
                        [
                            "_".join(col[0].split(" "))
                            for col in mysql_datas["table_details"]
                        ]
                    )
                ),
                mysql_datas["datas"],
            )

            return "success"
