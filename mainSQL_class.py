import mincemeat
import glob
import csv

def file_contents(file_name):
        f = open(file_name)
        try:
            return f.read()
        finally:
            f.close()
            
class MapReduce:
    
    def __init__(self, files, savepath, password = "changeme"):
        self.path_files = files
        self.password = password
        self.save_file = savepath
        self.vendas = self.path_files[0]
        self.filiais = self.path_files[1]
        self.server = mincemeat.Server()
        self.server.datasource = self.create_source()
        self.server.mapfn = self.mapfn
        self.server.reducefn = self.reducefn
        self.results = self.server.run_server(password=self.password)
        
    def create_source(self):
        return dict((file_name, file_contents(file_name)) for file_name in self.path_files)
    
    def mapfn(self, k, v):
        print (f'map {k}')
        for line in v.splitlines():
            if k == self.vendas:
                yield line.split(';')[0], 'Vendas' + ':' + line.split(';')[5]
            if k == self.filiais:
                yield line.split(';')[0], 'Filial' + ':' + line.split(';')[1]
    
    def reducefn(self, k, v):
        print(f'reduce {k}')
        return v
    
    def write(self):
        writer = csv.writer(open(self.save_file, "w"))
        for k, v in self.results.items():
            w.writerow([k, v])

path_files = glob.glob('D:\\Users\\1411202\\Desktop\\MapReduce-main\\join\\*')
save_path = r'D:\Users\1411202\Desktop\MapReduce-main\resultjoin.csv'

main = MapReduce(path_files, save_path)
main.write()