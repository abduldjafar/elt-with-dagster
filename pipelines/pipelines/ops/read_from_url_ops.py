import requests
from contextlib import closing
import csv
from dagster import op, Out
from services.clickhouse import ClickhouseServices

clickhouseSvc = ClickhouseServices()

def full_loads_from_url(
    name="default_name",
    db_name="default_db_name",
    tb_name="default_tb_name",
    order_by="",
    url="",
    chServer=None
):
    def csv_from_urls(url):
        headers = []
        datas = None
        with closing(requests.get(url, stream=True)) as r:
            f = (line.decode('utf-8') for line in r.iter_lines())
            reader = csv.reader(f, delimiter=',', quotechar='"')
            datas = [ headers.append(x) if i == 0 else tuple(x) for i,x in enumerate(reader)]
            yield  datas,headers
    
    @op(name=name, out={name: Out()})
    def load_datas():
        downloaded_datas = csv_from_urls(url)
        result,headers = next(downloaded_datas)
        result = result[1:]

        cleanned_headers = [datas for datas in headers][0]
        columns = ",".join([ "{} {}".format(column,"String") if column == order_by else  "{} Nullable({})".format(column,"String") for column in cleanned_headers ])

        return clickhouseSvc.insert_general_datas_to_clickhouse(
                tb_name,cleanned_headers,columns,db_name,order_by,result,chServer
            )
    
    return load_datas

        