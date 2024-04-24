import os
import sys
sys.path.insert(0, os.getcwd())
import settings
from knowledges.schema_loader import Loader
from tools.compare import similarity

# schema linking, for table, column
class Sch_linking:

    def __init__(self,scha_file):
        self.similarity = similarity()
        self.info_loader = Loader(scha_file)

    def substitute(self, term, tableName='', type='table'):

        if type not in ['table', 'column']:
            raise ValueError(f"Unknown type {type}")
        
        subst = {'conf': 0, 'new_term': ""}
        if type == 'table':
            tables = self.info_loader.tables
            for table in tables:
                db_name = table['table']
                temStr = f"{db_name},{table['table_zh']},{table['alias_en']},{table['alias_zh']}"
                termList = temStr.split(',')    
                s = self.similar(term,termList)
                if (s['score'] > subst['conf']):
                    subst['conf'] = s['score']
                    subst['new_term'] = db_name
            if subst['new_term']=='':
                subst['new_term'] = tables[0]['table']
            return subst

        columns_dict = self.info_loader.get_all_columns(tableName)
        for col in columns_dict:
            db_colName = col['column_en']
            temStr = f"{db_colName},{col['column_zh']},{col['alias_en']},{col['alias_zh']}"
            termList = temStr.split(',')
            s = self.similar(term,termList)
            if (s['score'] > subst['conf']):
                subst['conf'] = s['score']
                subst['new_term'] = db_colName

        if subst['new_term']=='':
            subst['new_term'] = columns_dict[0]['column_en']
        return subst
    
    def refine(self,slots1):

        slots = slots1.copy()
        tableName = slots['from']
        subst = self.substitute(tableName)
        tableName = slots['from'] = subst['new_term']
        if subst['conf'] < 0.5:
            slots['from'] += '?'

        columns = slots['columns']
        for idx, column in enumerate(columns):
            subst = self.substitute(column, tableName, 'column')
            columns[idx] = subst['new_term']
            if subst['conf'] < 0.5:
                columns[idx] += '?'

        # condictions
        for cond in slots['conditions']:
            subst = self.substitute(cond['column'], tableName, 'column')
            cond['column'] = subst['new_term']
            if subst['conf'] < 0.5:
                cond['column'] += '?'

        return slots


    def similar(self,term,candidates):
        lang = self.similarity.getLang(term)
        candidates = [c for c in candidates if self.similarity.getLang(c) == lang]
        matched = {'term':candidates[0], 'score':0}
       
        for ref in candidates:
            s = self.similarity.getUpperSimil(term,ref)
            if s > matched['score']:
                matched['score'] = s
                matched['term'] = ref
        return matched
    
# Example usage
if __name__ == '__main__':
    sch_linking = Sch_linking()
    slots = {'from': 'products', 'columns': ['brand name', 'item price'], 'conditions': [{'column': 'brand', 'op': '=', 'value': '联想'}]}
    print(slots)
    result = sch_linking.refine(slots)
    print(result)