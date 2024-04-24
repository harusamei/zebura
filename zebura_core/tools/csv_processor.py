import csv
import json
import os
import sys
class pcsv:
    
    def __init__(self):
        self.max_rows = 1000000

    def read_csv(self,csv_filename, rows=None):
        csv_rows = []
        if not rows or rows > self.max_rows or rows < 0:
            rows = self.max_rows
        try:
            with open(csv_filename, 'r', encoding='utf-8-sig') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    row = {k.lstrip('\ufeff'): v for k, v in row.items()}
                    if any(field.strip() for field in row.values()):  # Check if the row is not empty
                        csv_rows.append(row)
                    if len(csv_rows) >= rows:
                        break
        except Exception as e:
            print(f"Error: {e}")
        
        return csv_rows

    def write_csv(self,csv_rows, csv_filename):
        with open(csv_filename, 'w', newline='',encoding='utf-8-sig') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=csv_rows[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(csv_rows)

    def csv2jsonfile(self,csv_rows, json_filename):
        with open(json_filename, 'w') as json_file:
            json.dump(csv_rows, json_file, indent=4)

    def csv2json(self,csv_rows):
        return json.dumps(csv_rows, indent=4, ensure_ascii=False)

    def oneRow2json(self,csv_row):
        # 不加ensure_ascii=False的话，中文会被转换为Unicode编码,\uXXXX
        return json.dumps(csv_row, indent=4, ensure_ascii=False)

    def json2dict(self,json_str):
        return json.loads(json_str)

    def dict2json(self,dict):
        return json.dumps(dict, indent=4, ensure_ascii=False)
    
    def deleteKey(self,csv_rows, key):
        for row in csv_rows:
            row.pop(key)

if __name__ == '__main__':

    my_pcsv = pcsv()
    sys.path.insert(0, os.getcwd().lower())
    curPath = os.getcwd().lower()
    csv_filename = 'datasets\\goodcases_schema.csv'
    print(csv_filename)
    csv_rows = my_pcsv.read_csv(csv_filename)
    # aJson=my_pcsv.oneRow2json(csv_rows[0])
    allJson=my_pcsv.csv2json(csv_rows)

    temDict= {'table':'none','es_index':'none'}
    temDict['columns'] =my_pcsv.json2dict(allJson)
    
    with open(curPath+'\\datasets\\goodcases_info_tables.json', 'w',encoding='utf-8-sig') as json_file:
        json_file.write(my_pcsv.dict2json({'tables':[temDict]}))
           
    
