import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import urllib.request
import pdb
from selenium.webdriver.chrome.options import Options
from lxml import html
import getpass
class FindNoDPASINS:
    def __init__(self):
        self.url = 'https://1234'
        self.chrome_options = Options()
        if getpass.getuser() =='xinyyang':
            self.chrome_options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        # self.chrome_options.binary_location = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
        self.chrome_path = os.environ['USERPROFILE'] + r'\AppData\Local\Google\Chrome\User Data'
        self.chrome_options.add_argument("user-data-dir=" + self.chrome_path)
        # self.chrome_options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        self.driver = webdriver.Chrome(options=self.chrome_options)
        self.arc = 3240
        self.current_path = os.getcwd()
        #asin_frame用来存放没有DP的ASIN和其在CSI上的blocker信息的变量
        self.asin_frame = pd.DataFrame(columns=['ASIN','DP_Status','DataAugmenter'])
        self.final_asin_info = pd.DataFrame(columns=['ASIN','Source','Target','Blocker','Blocker_content','Tracker'])
        self.source_scope = {'US':1,'UK':3,'DE':4,'JP':6,'GB':3}
        self.target_scope = {'CN':3240}
        self.code_source_mapping = {1:'US',3:'UK',4:'DE',6:'JP'}
        self.code_target_mapping = {3240:'CN'}
    def read_inflow_convert_txt(self,inflow):
        file_path = os.getcwd()
        asin_list = inflow['ASIN'].tolist()
        for i in range(len(asin_list)):
            asin_list[i] = str(asin_list[i])
        asins = '\n'.join(asin_list)
        with open('check_dp.txt','w') as f:
            f.writelines(asins)
        return inflow

    def upload_asins_and_download_result(self):
        self.driver.get(self.url)
        WebDriverWait(self.driver, 120).until(EC.title_contains("Browse Query Editor"))
        input_button = WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="redux-app"]/div/div[1]/div[2]/div/div/div/div[1]/div/input')))
        input_button.send_keys(os.getcwd()+'/'+'check_dp.txt')
        export_button = WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="redux-app"]/div/div[1]/div[2]/div/div/div/div[2]/div[4]/div[1]/div/button')))
        self.driver.execute_script("arguments[0].click();",export_button)
        export_button1 = WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div[2]/button[2]')))
        self.driver.execute_script("arguments[0].click();", export_button1)
        # import pdb
        # pdb.set_trace()
        export_link = WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div[2]/div/div/div[2]/a')))
        self.driver.get('https://browse-query-editor-cn.aka.amazon.com/exportTaskList')
        target_spot = WebDriverWait(self.driver,90).until(EC.presence_of_element_located((By.XPATH,'//*[@id="redux-app"]/div/table/tbody[1]/tr/td[10]')))
        while True:
            try:
                target_spot = WebDriverWait(self.driver,5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="redux-app"]/div/table/tbody[1]/tr/td[10]/a')))
                break
            except:
                self.driver.refresh()
        download_link = target_spot.get_attribute('href')
        urllib.request.urlretrieve(download_link,os.getcwd()+'/'+'CSI_Result.csv')

    def find_asins_without_dp(self):
        file = pd.read_csv(os.getcwd()+'/'+'CSI_Result.csv')
        asins = file[file['item_name']=='INVALID ASIN']['ASIN'].tolist()
        return asins

    def find_dp_status(self,asins):
        for asin in asins:
            augment_info = []
            url = 'https://csi.amazon.com/view?view=blame_o&item_id=' + str(asin) + '&marketplace_id=' + str(self.arc) + '&stage=prod&search_string=website_rejected'
            self.driver.get(url)
            asin_status = WebDriverWait(self.driver,30).until(EC.presence_of_element_located((By.XPATH,'/html/body/div[2]/table/tbody/tr/td[3]/div/table[2]/tbody/tr/td[1]/fieldset'))).text
            # print(asin_status)

            html_content = WebDriverWait(self.driver,30).until(EC.presence_of_element_located((By.XPATH,'//*[@id="productdata"]'))).get_attribute('innerHTML')
            xml = html.fromstring(html_content)
            #在status=Active的时候xml.xpath("//tbody")[0]会报IndexError的错误,在status=not discoverable的时候xml.xpath("//tbody")[0]会读到data-augmenter的相关表格信息
            try:
                table = xml.xpath("//tbody")[0]
                for row in table.xpath("//tr"):
                    augment_info.append([td.text for td in row.xpath(".//td[text()]")])
                augment_flag = 0
                for eachinfo in augment_info:
                    if eachinfo != []:
                        if ('website_rejected'in eachinfo[1]) and ('merchant' in eachinfo[1]) and ('data-augmenter' in eachinfo[1]):
                            augment_flag =1
                            break
                        else:
                            continue
            except:
                augment_flag=0
            if augment_flag == 1:
                self.asin_frame = self.asin_frame.append({'ASIN': asin, 'DP_Status': asin_status,'DataAugmenter':'YES'}, ignore_index=True)
            else:
                self.asin_frame = self.asin_frame.append({'ASIN': asin, 'DP_Status': asin_status,'DataAugmenter':'NO'}, ignore_index=True)
        self.driver.quit()

    def find_restricted_by_rps(self):
        restricted_by_rps = self.asin_frame[self.asin_frame['DP_Status'].str.contains('Public Facing Restricted Reason')]
        restricted_by_rps1 = self.asin_frame[self.asin_frame['DP_Status'].str.contains('Restricted by RPS')]
        restricted_by_rps = pd.concat([restricted_by_rps,restricted_by_rps1],ignore_index=True)
        restricted_by_rps.drop_duplicates(['ASIN'],inplace=True)
        print(self.asin_frame)
        for index, asin_info in restricted_by_rps.iterrows():
            if '经认定，此商品包含非法信息，为高风险商品，因此无法申诉' in asin_info['DP_Status']:
                self.final_asin_info = self.final_asin_info.append({'ASIN':asin_info['ASIN'],'Source':'None','Target':'3240','Blocker':'Restricted by RPS','Blocker_content':asin_info['DP_Status'],'Tracker':'non_action'},ignore_index=True)
            elif '经认定，此商品的 ASIN 无效，请创建详情页面并向 PS&FS 团队提交申诉' in asin_info['DP_Status']:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240', 'Blocker': 'Restricted by RPS',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'non_action'},ignore_index=True)
            elif '上述商品中被认定为“禁止携带、邮寄进境的动植物及其产品' in asin_info['DP_Status']:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240', 'Blocker': 'Restricted by RPS',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'non_action'},ignore_index=True)
            elif '经认定，此商品为电压商品，缺少 ASIN，请添加相关信息并向 PS&FS 团队提交申诉' in asin_info['DP_Status']:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240', 'Blocker': 'Restricted by RPS',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'Need backfill Voltage'},ignore_index=True)
            elif '此ASIN 因缺少成分信息而被判定为高风险产品，请添加相关信息提交申诉TT' in asin_info['DP_Status']:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240', 'Blocker': 'Restricted by RPS',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'Need backfill Ingredient information'},ignore_index=True)
            else:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240', 'Blocker': 'Restricted by RPS',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'Appeal tracker'},ignore_index=True)
        rps_asins = restricted_by_rps['ASIN'].tolist()
        self.asin_frame = self.asin_frame[~self.asin_frame.isin(rps_asins)['ASIN']]
        # print(self.asin_frame)

    def find_recall_product_safety(self):
        recall_product_safety = self.asin_frame[self.asin_frame['DataAugmenter']=='YES']
        recall_product_safety1 = self.asin_frame[self.asin_frame['DP_Status'].str.contains('Public Facing Recall Reason')]
        recall_product_safety = pd.concat([recall_product_safety,recall_product_safety1],ignore_index=True)
        recall_product_safety.drop_duplicates(['ASIN'], inplace=True)
        for index, asin_info in recall_product_safety.iterrows():
            if 'PUBLIC_MARKET_WITHDRAWAL' in asin_info['DP_Status']:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240', 'Blocker': 'Recall (Product_Safety)',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'non_action'},
                    ignore_index=True)
            else:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240',
                     'Blocker': 'Recall (Product_Safety)',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'Appeal tracker'},
                    ignore_index=True)
        recall_asins = recall_product_safety['ASIN'].tolist()
        self.asin_frame = self.asin_frame[~self.asin_frame.isin(recall_asins)['ASIN']]

    def find_create_dp(self):
        for index, asin_info in self.asin_frame.iterrows():
            if 'Item was discontinued on' in asin_info['DP_Status']:
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240',
                     'Blocker': 'Create DP',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'Appeal tracker'},
                    ignore_index=True)
            else:
            #最后判断create dp的blocker，所以如果blocker不是create dp，则不在restricted by rps;recall;create dp三种范围内，判断为UNKNOWN DP Blocker
                self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin_info['ASIN'], 'Source': 'None', 'Target': '3240',
                     'Blocker': 'UNKNOWN DP Blocker',
                     'Blocker_content': asin_info['DP_Status'], 'Tracker': 'Appeal tracker'},
                    ignore_index=True)

    def match_source_target(self,inflow):
        inflow.drop_duplicates(['ASIN','Source','Target'], inplace=True)
        self.final_asin_info.drop(columns=['Source','Target'],inplace=True)
        self.final_asin_info = self.final_asin_info.merge(inflow,how='left',on='ASIN')

    def create_output(self):
        # self.final_asin_info.drop(['DestinationBuyable','SourceBuyable'],inplace=True,axis=1)
        self.final_asin_info = self.final_asin_info[['ASIN','Source','Target','Blocker','Blocker_content','Tracker']]
        # self.final_asin_info.to_excel(os.getcwd()+'/'+'DP_output.xlsx',index=False)
        self.final_asin_info.to_csv(os.getcwd()+'/'+'DP_output.xlsx',index=False)


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

    def find_dp_or_not(self,asin_list):
        for asin in asin_list:
            self.final_asin_info = self.final_asin_info.append(
                    {'ASIN': asin, 'Source': 'None', 'Target': '3240',
                     'Blocker': 'DP or not',
                     'Blocker_content': 'NO DP', 'Tracker': 'Appeal tracker'},
                    ignore_index=True)

    def convert_arc_to_number(self):

        self.final_asin_info = self.final_asin_info[~self.final_asin_info['Source'].isnull()]
        self.final_asin_info.reset_index(drop=True, inplace=True)
        for i in range(len(self.final_asin_info)):
            self.final_asin_info.loc[i,'Source'] = self.source_scope[self.final_asin_info.loc[i,'Source']]
            self.final_asin_info.loc[i,'Target'] = self.target_scope[self.final_asin_info.loc[i,'Target']]


    def execution(self):
        self.clean_all_csv()
        self.clean_all_txt()
        original_df = self.read_inflow_convert_txt(inflow= pd.read_excel(os.getcwd()+'/'+'input.xlsx'))
        self.upload_asins_and_download_result()
        asins = self.find_asins_without_dp()
        # self.find_dp_status(asins)
        # self.find_restricted_by_rps()
        # self.find_recall_product_safety()
        # self.find_create_dp()

        self.find_dp_or_not(asins)
        # pdb.set_trace()
        self.match_source_target(original_df)
        # try:
        #     self.final_asin_info['Source'] = self.final_asin_info['Source'].map(lambda x:self.code_source_mapping[x])
        #     self.final_asin_info['Target'] = self.final_asin_info['Target'].map(lambda x:self.code_target_mapping[x])
        # except:
        #     pass
        # self.create_output()
        # print(self.final_asin_info)
        self.driver.quit()
        # self.convert_arc_to_number()
        self.final_asin_info.to_excel('csi_no_dp_asins_result.xlsx',index=False)
        # return self.final_asin_info
if __name__ == '__main__':
    findnodpasins = FindNoDPASINS()
    # findnodpasins.clean_all_csv()
    # findnodpasins.clean_all_txt()
    # original_df = findnodpasins.read_inflow_convert_txt()
    # findnodpasins.upload_asins_and_download_result()
    # asins = findnodpasins.find_asins_without_dp()
    # findnodpasins.find_dp_status(asins)
    # findnodpasins.find_restricted_by_rps()
    # findnodpasins.find_recall_product_safety()
    # findnodpasins.find_create_dp()
    # findnodpasins.match_source_target(original_df)
    # findnodpasins.create_output()
    findnodpasins.execution()