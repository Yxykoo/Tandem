import pandas as pd
import os
import pdb
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
import json
import re
import sys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import getpass
class HandleInflow:
    def __init__(self):
        self.current_path = os.getcwd()
        self.input_address = self.current_path + '/input.xlsx'
        self.output_address = self.current_path + '/Bulk_output.xlsx'
        #每个txt放700个asin，因为bulk lifecycle有txt文档的大小限制
        self.asin_num_limit_per_txt = 700
        self.inflow = pd.read_excel(self.input_address)
        self.country_code_mapping = {'US':1,'DE':4,'JP':6,'UK':3,'CN':3240,'GB':3}
        self.code_country_mapping = {1:'US',4:'DE',6:'JP',3:'GB',3240:'CN'}
        # pdb.set_trace()
        # self.inflow['DestinationBuyable'] = self.inflow['DestinationBuyable'].astype('str')
        # self.inflow['SourceBuyable'] = self.inflow['SourceBuyable'].astype('str')
        # print(self.inflow.loc[1,'DestinationBuyable'])
        # print(type(self.inflow.loc[1,'DestinationBuyable']))
    def change_input_file_format(self):
        # df = pd.read_excel(self.input_address)
        self.inflow['ASIN'] = self.inflow['ASIN'].map(lambda x: str(x).zfill(10))
        self.inflow['Source'] = self.inflow['Source'].map(lambda x: self.country_code_mapping[x])
        self.inflow['Target'] = self.inflow['Target'].map(lambda x: self.country_code_mapping[x])
        self.inflow = self.inflow[['ASIN','Source','Target']]
        self.inflow.to_excel(self.input_address,index=False)

    def change_back_input_file_format(self):

        self.inflow['Source'] = self.inflow['Source'].map(lambda x: self.code_country_mapping[x])
        self.inflow['Target'] = self.inflow['Target'].map(lambda x: self.code_country_mapping[x])
        self.inflow = self.inflow[['ASIN', 'Source', 'Target']]
        self.inflow.to_excel(self.input_address,index=False)
    def conversion(self, data):
        for i in range(len(data.index)):
            if 'UK' in data.iloc[i]['Source']:
                data.at[i, 'Source'] = 'GB'
            if 'UK' in data.iloc[i]['Target']:
                data.at[i, 'Target'] = 'GB'
        return data

    def convert_to_txt(self,inflow,num,arc):
        #inflow为asin的list
        #inflow 203 num 100
        asin_sum = len(inflow)
        group_num = asin_sum//num
        for i in range(group_num):
            f = open(arc+'{}'.format(i+1)+'.txt','w')
            for asin in inflow[i*num:(i+1)*num]:
                f.write(str(asin) + '\n')
            f.close()
        if asin_sum%num!=0:
            f = open(arc + '{}'.format(group_num + 1) + '.txt', 'w')
            for asin in inflow[group_num*num:]:
                f.write(str(asin) + '\n')
            f.close()


    def clean_all_txt(self):
        file_list = os.listdir(self.current_path)
        # import pdb
        # pdb.set_trace()
        for file in file_list:
            if file[-3:] == 'txt':
                os.remove(self.current_path+'/'+file)

    def clean_all_csv(self):
        file_list = os.listdir(self.current_path)
        for file in file_list:
            if file[-3:] == 'csv':
                os.remove(self.current_path+'/'+file)

    def get_buyable_in_destination_asins(self):

        # 把inflow中的ASIN转换为10位的功能
        self.inflow = self.make_sure_ten_digits(self.inflow)
        # 将UK变成GB
        self.inflow = self.conversion(self.inflow)
        buyable_asins = self.inflow[self.inflow['DestinationBuyable']==1]
        self.inflow = self.inflow[self.inflow['DestinationBuyable']!=1]
        self.inflow.drop(columns=['DestinationBuyable','SourceBuyable'],inplace=True)
        return buyable_asins



    def read_input_convert_txt(self):
        # print(self.inflow)
        #把inflow中的ASIN转换为10位的功能

        self.inflow = self.make_sure_ten_digits(self.inflow)
        # self.inflow
        #将UK变成GB
        self.inflow = self.conversion(self.inflow)
        self.inflow['ARC'] = self.inflow['Source'] + '-' + self.inflow['Target']
        #去重
        self.inflow.drop_duplicates(subset=['ASIN','Source','Target'],inplace=True)

        arc_list = self.inflow['ARC'].tolist()

        #arc_list去重
        arc_list = list(dict.fromkeys(arc_list))
        #删除当前目录下所有的txt文件
        self.clean_all_txt()
        self.clean_all_csv()
        #分arc创建txt并加入asin
        for arc in arc_list:
            asins = self.inflow[self.inflow['ARC']==arc]['ASIN'].tolist()
            self.convert_to_txt(asins,self.asin_num_limit_per_txt,arc)
        return self.inflow

    def get_arc_list(self,data):
        arc_list = data['ARC'].tolist()
        arc_list = list(dict.fromkeys(arc_list))
        return arc_list

    def generate_report(self,link_list):
        inflow = pd.read_excel(self.input_address)
        inflow = self.conversion(inflow)
        inflow['Request Link'] = ''
        for i in range(len(inflow.index)):
            arc = inflow.iloc[i]['Source'] + '-' + inflow.iloc[i]['Target']
            for link in link_list:

                if arc in link:
                    inflow.at[i,'Request Link'] = link
        print(inflow)
        inflow.to_excel(self.current_path + '//Output_yankunyank.xlsx',index=False)

    def concat_files(self,path_list):
        df_list =[]
        for path in path_list:
            df = pd.read_csv(path,error_bad_lines=False)
            df_list.append(df)
        result = pd.concat(df_list,ignore_index=True)
        return result

    def find_retail_inflow(self,data):
        return data[data['merchant_type']=='retail']

    def find_all_inflow(self,buyable_asins_arc_list):

        self.inflow = self.inflow[~((self.inflow['ASIN'].isin(buyable_asins_arc_list[0]))&(self.inflow['Source'].isin(buyable_asins_arc_list[1])))]
        return self.inflow

    def make_sure_ten_digits(self,data):
        for i in range(len(data.index)):
            if len(str(data.iloc[i,0]))<10:
                num = 10 - len(data.iloc[i,0])
                data.iloc[i,0] = num * '0' + str(data.iloc[i,0])

        return data
    def filter_buyable_inflow(self,buyable_asins_arc_list,retail_inflow):
        arc_to_num_mapping = {'DE':4,'JP':6,'US':1,'UK':3}
        buyable_arc_num_list = []
        for arc in buyable_asins_arc_list[1]:
            buyable_arc_num_list.append(arc_to_num_mapping[arc])
        #buyable_asins_arc_list中存了两个list，第一个list存的是buyable的ASIN，第二个list存的是ASIN对应的source国家的代码，这里用buyable_arc_num_list转成数字
        return retail_inflow[~((retail_inflow['ASIN'].isin(buyable_asins_arc_list[0]))&(retail_inflow['Buyable in destination']=='[YES]')&(retail_inflow['SourceMarketplace'].isin(buyable_arc_num_list)))]

    def convert_country_digit_to_str(self,input_file):
        source_mapping = {4:'DE',6:'JP',1:'US',3:'UK'}
        target_mapping = {3240:'CN'}
        input_file['Target'] = input_file['TargetMarketplace'].apply(lambda x:target_mapping[x])
        input_file['Source'] = input_file['SourceMarketplace'].apply(lambda x:source_mapping[x])
        return input_file

    def get_buyable_asin_arc_list(self,df):
        asin_arc_list = []
        asin_list = []
        arc_list = []
        for i in df.index:
            asin_list.append(df.loc[i,'ASIN'])
            arc_list.append(df.loc[i,'Source'])
        asin_arc_list.append(asin_list)
        asin_arc_list.append(arc_list)
        return asin_arc_list

    def get_oculus_inflow(self,oculus_file_path):
        oculus_file_df = pd.read_csv(oculus_file_path, error_bad_lines=False)
        oculus_file_df = oculus_file_df[oculus_file_df['DP_DestinationBuyable'] != 1][['asin', 'ARC', 'GS']]
        oculus_file_df.rename(columns={'asin': 'ASIN', 'ARC': 'Source', 'GS': 'Target'}, inplace=True)
        oculus_file_df.to_excel('input.xlsx', index=False)

    def get_sim_inflow(self):
        deal_json = 'https://12345'
        chrome_options = Options()
        if getpass.getuser() =='xinyyang':
            chrome_options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        # self.chrome_options.binary_location = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
        chrome_path = os.environ['USERPROFILE'] + r'\AppData\Local\Google\Chrome\User Data'
        chrome_options.add_argument("user-data-dir=" + chrome_path)
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(deal_json)
        deal_element = driver.find_elements_by_tag_name('body')[0]
        json_deal = json.loads(deal_element.text)
        final_asins_list = []
        for sim_block in json_deal['documents']:
            # print(sim_block['id'])
            driver.get('https://12345.amazon.com/issues?id.1=' + str(
                sim_block['id']) + '&11111')
            sim_block_element = driver.find_elements_by_tag_name('body')[0]
            sim_block_json_test = json.loads(sim_block_element.text)
            line = []
            if (sim_block_json_test['documents'][0]['status'] == 'Open'):

                try:
                    description = sim_block_json_test['documents'][0]['description']
                    line_list = description.split('\n')
                    sim_lines = []
                    source_list = ['US', 'UK', 'DE', 'JP']
                    for line in line_list:
                        sim_line = []
                        rool = re.compile('\s+')
                        if (len(rool.split(line)) == 2) and (rool.split(line)[-1].strip() in source_list):
                            sim_line.extend(rool.split(line))
                            sim_line.extend(['CN'])
                            sim_lines.append(sim_line)
                    final_asins_list.extend(sim_lines)
                            # print(sim_line)
                except:
                    pass
        # pdb.set_trace()
        driver.quit()
        if final_asins_list !=[]:
            columns_name = ['ASIN','Source','Target']
            sim_df = pd.DataFrame(data=final_asins_list,columns=columns_name)
            sim_df.to_excel('input.xlsx',index=False)
        else:
            print('未抓到SIM中的ASIN，程序结束')
            sys.exit()
        # pdb.set_trace()



if __name__ == '__main__':
    handleinflow = HandleInflow()
    # path_list = [os.getcwd()+r'/'+'GB-US_93563ad4-2f31-498c-9058-383eb92afcb9.csv',os.getcwd()+r'/'+'GB-US_cc007b65-be26-4355-a935-c47a10ca7bee.csv',os.getcwd()+r'/'+'US-GB_1d1fc709-773a-478d-8d25-ba05f11d6817.csv']
    # result = handleinflow.concat_files(path_list)
    # print(result)
    handleinflow.clean_all_txt()
