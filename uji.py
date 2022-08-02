import csv


def csv_from_file(file_path):
        headers = []
        datas = None
        with open(file_path, 'r') as r:
            
            f = r.readlines()
            for data in f:
                print(data)
            reader = csv.reader(f, delimiter=',', quotechar='"')
            datas = [ headers.append(x) if i == 0 else tuple(x) for i,x in enumerate(reader)]

csv_from_file("/Users/abdulharisdjafar/Downloads/sales.csv")