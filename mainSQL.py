import mincemeat
import glob
import csv

text_files = glob.glob('D:\\Users\\1411202\\Desktop\\MapReduce-main\\join\\*')


def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()
        
        
source = dict((file_name, file_contents(file_name)) for file_name in text_files)

def mapfn(k, v):
    print (f'map {k}')
    for line in v.splitlines():
        if k == 'D:\\Users\\1411202\\Desktop\\MapReduce-main\\join\\2.2-vendas.csv':
            yield line.split(';')[0], 'Vendas' + ':' + line.split(';')[5]
        if k == 'D:\\Users\\1411202\\Desktop\\MapReduce-main\\join\\2.2-filiais.csv':
            yield line.split(';')[0], 'Filial' + ':' + line.split(';')[1]

def reducefn(k, vs):
    print(f'reduce {k}')
    total = 0
    for item in vs:
        if item.split(':')[0] == 'Vendas':
            total += int(item.split(':')[1])
        if item.split(':')[0] == 'Filial':
            Nomefilial = item.split(':')[1]
    L = []
    L.append(f'{Nomefilial} , {str(total)}')
    return L

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("D:\\Users\\1411202\\Desktop\\MapReduce-main\\resultjoin.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])