#basesolve.com
import pandas as pd
import os,json,argparse,re

class service_automation():
    def __init__(self, excel, csv, input_path, output_path):
        self.excel = excel
        self.csv = csv
        self.input_path = input_path
        self.output_path = output_path

    def get_indices(self,line):
        index_dict={
            "order_date":0,"p_mrn":0,"a_mrn":0,"patient_name":""
        }
        i=0
        for element in line.split(','):
            if "order" in element:
                index_dict["order_date"]=i
            elif "patient mrn" in element:
                index_dict["p_mrn"]=i
            elif "account mrn" in element:
                index_dict["a_mrn"]=i
            elif "patient name" in element:
                index_dict["patient_name"]=i
            i+=1
        return index_dict

    def process(self):
        
        excel_file = os.path.join(self.input_path+self.excel)
        csv_file = os.path.join(self.input_path+self.csv)

        excel_df=pd.read_excel(excel_file)

        excel_df.to_csv(r'temp.csv')
        flagg = False
        excel_csv_file = open('temp.csv','r').readlines()
        excel_dict={}
        for line in excel_csv_file:
            if "order date" in line.lower() and "patient mrn" in line.lower():
                flagg=True
                index_dict = self.get_indices(line.lower())
            elif flagg:
                row=line.split(',')
                temp_dict={}
                temp_dict['data_time']=str(row[index_dict['order_date']])
                temp_dict['patient_name_age']=row[index_dict['patient_name']]
                temp_dict['sample_id']=row[index_dict['a_mrn']]
                temp_dict['patient_MRN']=row[index_dict['p_mrn']]
                excel_dict[row[index_dict['p_mrn']]]=temp_dict
        
        csv_df=pd.read_csv(csv_file)
        csv_dict={}
        for index, row in csv_df.iterrows():
            temp_dict={}
            temp_dict['A/1']=row['A/1']
            temp_dict['A/2']=row['A/2']
            if temp_dict['A/2']=='-': temp_dict['A/2']=temp_dict['A/1']
            temp_dict['B/1']=row['B/1']
            temp_dict['B/2']=row['B/2']
            if temp_dict['B/2']=='-': temp_dict['B/2']=temp_dict['B/1']
            temp_dict['C/1']=row['C/1']
            temp_dict['C/2']=row['C/2']
            if temp_dict['C/2']=='-': temp_dict['C/2']=temp_dict['C/1']
            temp_dict['DRB1/1']=row['DRB1/1']
            temp_dict['DRB1/2']=row['DRB1/2']
            if temp_dict['DRB1/2']=='-': temp_dict['DRB1/2']=temp_dict['DRB1/1']
            temp_dict['DQB1/1']=row['DQB1/1']
            temp_dict['DQB1/2']=row['DQB1/2']
            if temp_dict['DQB1/2']=='-': temp_dict['DQB1/2']=temp_dict['DQB1/1']
            csv_dict[str(row['MRNO'])]=temp_dict

        merged_dict={}
        for key_excel, value_excel in excel_dict.items():
            new_dict=value_excel
            try:
                key_csv_value=csv_dict[key_excel]
                if key_csv_value!={}:
                    new_dict.update({"allele_record" : key_csv_value})
                merged_dict[value_excel['sample_id']]=new_dict
            except KeyError:
                continue
        with_serotype = serology().stream(merged_dict)
        return self.make_csv(with_serotype)

    def make_csv(self, json_dict):
        sample_pass_list=[]
        sample_fail_list=[]
    
        for sample_id, sample_values in json_dict.items():
            sample_name=""
            try:
                sample_name=sample_values["patient_name_age"].split()[1]
            except IndexError:
                pass
            file_name = os.path.join(self.output_path, sample_id+"_"+sample_name.upper())
            try:
                out = open(file_name+"_out.csv",'w')
                out.write(file_name+"\n")
                out.write(sample_values["A/1"]+","+sample_values["A/1_serotype"]+",A1\n")
                out.write(sample_values["A/2"]+","+sample_values["A/2_serotype"]+",A2\n")
                out.write(sample_values["B/1"]+","+sample_values["B/1_serotype"]+",B1\n")
                out.write(sample_values["B/2"]+","+sample_values["B/2_serotype"]+",B2\n")
                out.write(sample_values["C/1"]+","+sample_values["C/1_serotype"]+",C1\n")
                out.write(sample_values["C/2"]+","+sample_values["C/2_serotype"]+",C2\n")
                out.write(sample_values["DRB1/1"]+","+sample_values["DRB1/1_serotype"]+",DR1\n")
                out.write(sample_values["DRB1/2"]+","+sample_values["DRB1/2_serotype"]+",DR2\n")
                out.write(sample_values["DQB1/1"]+","+sample_values["DQB1/1_serotype"]+",DQ1\n")
                out.write(sample_values["DQB1/2"]+","+sample_values["DQB1/2_serotype"]+",DQ2")
                out.close()
                sample_pass_list.append(sample_id)
            except Exception:
                sample_fail_list.append(sample_id)
        return sample_pass_list, sample_fail_list

class serology:
    def __init__(self) -> None:
        pass
    
    def stream(self, allele_data):
        serotype_list = [x.strip() for x in open("rel_dna_ser.txt","r").readlines() if '#' not in x.strip()]
        serotype_dict = {";".join(y.split(';')[0:2]) : (y.split(';')[2]) for y in serotype_list}
        return self.extract(allele_data, serotype_dict)
    
    def transfer(self, locus, value,serotype_dict):
        value=value[value.find("*") + 1:]
        map_dict_locus = {"A/1":"A*;",
                          "A/2":"A*;",
                          "B/1":"B*;",
                          "B/2":"B*;",
                          "C/1":"C*;",
                          "C/2":"C*;",
                          "DRB1/1":"DRB1*;",
                          "DRB1/2":"DRB1*;",
                          "DQB1/1":"DQB1*;",
                          "DQB1/2":"DQB1*;"
                          }
        value=value.replace('G','')
        pattern = map_dict_locus[locus]+value
        obtained_serology="NULL"
        for k, v in serotype_dict.items():
            if re.search(r'[^0?]', v) and pattern in k:
                if obtained_serology=="NULL": obtained_serology=v
        if obtained_serology!="NULL":
            if pattern[0]=='C': obtained_serology="Cw"+str(obtained_serology)
            elif pattern[0]=='D': obtained_serology=pattern[0:2]+str(obtained_serology)
            else: obtained_serology=pattern[0]+str(obtained_serology)
        return obtained_serology
    
    def extract(self, allele_data, serotype_dict):
        combined_dict={}
        for sample_id,sample_records in allele_data.items():
            sample_dict={}
            for record_keys, record_values in sample_records.items():
                if record_keys=="allele_record":
                    for locus, locus_value in record_values.items():
                        serotype_value = self.transfer(locus , locus_value, serotype_dict)
                        serotype_key = locus+"_serotype"
                        sample_dict[locus]=locus_value
                        sample_dict[serotype_key]=serotype_value
                else:
                    sample_dict[record_keys]=record_values
            combined_dict[sample_id]=sample_dict
        return combined_dict
