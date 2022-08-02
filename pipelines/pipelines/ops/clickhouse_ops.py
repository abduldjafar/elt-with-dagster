from dagster import op, Out


def clickhouse_insert_datas(name="default", datas=(), ch_server=None):
    @op(name=name, out={name: Out()})
    def insert_datas_to_clickhouse(datas):
        mysql_datas = datas[0]
        tb_name = datas[1]
        db_name = datas[2]

        clickhouse_columns = ",".join(
            [
                "{} {}".format(
                    columns[0],
                    ch_server.convert_data_type_from_mysql()[columns[1].upper()],
                )
                for columns in mysql_datas["table_details"]
            ]
        )
        order_by = mysql_datas["table_details"][0][0]

        ch_server.create_table_with_columns(
            tb_name, clickhouse_columns, db_name, order_by
        )
        ch_server.insert_data(
            db_name,
            tb_name,
            "({})".format(",".join([col[0] for col in mysql_datas["table_details"]])),
            mysql_datas["datas"],
        )

        return "success"

    return insert_datas_to_clickhouse
