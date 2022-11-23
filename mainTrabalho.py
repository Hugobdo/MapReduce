import mincemeat
import glob
import csv

text_files = glob.glob('D:\\Users\\1411202\\Desktop\\MapReduce-main\\trabalho_1\\*')


def file_contents(file_name):
    f = open(file_name, encoding='UTF-8')
    try:
        return f.read()
    finally:
        f.close()
        
        
source = dict((file_name, file_contents(file_name)) for file_name in text_files)

def mapfn(k, v):
    print (f'map {k}')
    from stopwords import allStopWords
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    for line in v.splitlines():
        for author in line.split(':::')[-2].split('::'):
            for word in line.split(':::')[-1].split():
                no_punc = ""
                for char in word:
                   if char not in punctuations:
                       no_punc = no_punc + char
                if no_punc not in allStopWords:
                    key = f'{author};{no_punc}'
                    yield key, 1

def reducefn(k, vs):
    print(f'reduce {k}')
    return sum(vs)

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("D:\\Users\\1411202\\Desktop\\MapReduce-main\\result_trabalho_1.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])