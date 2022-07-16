
# coding: utf-8

import json
import pandas as pd
import os
import subprocess
import time
from status_data_processor_v2_chuanlian import *
from multiprocessing import Process, Pool
import pdb
class fetch_from_API(object):
    def __init__(self):
        self.line_list = []
#
    def call(self, pairs):
        success = False
        retry = 0
        line_R = []
        line_F = []
        while not success:
            try:
                asin = pairs[0]
                source_marketplace_id = pairs[1]
                target_marketplace_id = pairs[2]
                # asin = 'B09KQZWG5J'
                # source_marketplace_id = '4'
                # target_marketplace_id = '338851'

                cmd = '''awscurl -H "Content-Type: application/x-amz-json-1.1" "https://1234.execute-api.us-east-1.amazonaws.com/prod/1234"''' % (str(source_marketplace_id), str(target_marketplace_id), str(asin))
                p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                status_payload = p.communicate()
                status_json = json.loads(json.loads(status_payload[0])['result'][0])
                [line_R,line_F,yank_info] = StatusDataProcessor(status_json).apply()

                success = True
            except Exception as exc:
                # print("Error fetching status payload for ASIN {}".format(asin), str(exc))
                retry += 1
                if retry == 10:
                    print('Retried 10 times:', str(asin), str(source_marketplace_id), str(target_marketplace_id), str(exc))
                    break

        if not line_R and not line_F:
            line_R_str = ','.join(
                [str(asin), str(source_marketplace_id), str(target_marketplace_id), 'retail', '[Record Not Found]',
                 '[Record Not Found]', '[Record Not Found]', '[Record Not Found]', '[Record Not Found]',
                 '[Record Not Found]', '[Record Not Found]','[Record Not Found]', '[Record Not Found]'])
            line_F_str = ','.join(
                [str(asin), str(source_marketplace_id), str(target_marketplace_id), 'agl', '[Record Not Found]',
                 '[Record Not Found]', '[Record Not Found]', '[Record Not Found]', '[Record Not Found]',
                 '[Record Not Found]', '[Record Not Found]','[Record Not Found]', '[Record Not Found]'])
        else:
            yank_info = str(yank_info).replace(',',';')
            line_R.append(yank_info)
            line_F.append(yank_info)
            line_R_str = ','.join(line_R)
            line_F_str = ','.join(line_F)
        return line_R_str,line_F_str

    def get_exportability(self,x):
        if (x['aglExportability'] == '[YES]') or (x['retailExportability'] == '[YES]'):
            return '[YES]'
        elif (x['aglExportability'] == '[Record Not Found]') and (x['retailExportability'] == '[Record Not Found]'):
            return '[Record Not Found]'
        else:
            return '[NO]'
    def api_main(self, input_df):
        input_df = input_df.loc[:, ['ASIN', 'Source', 'Target']]
        input_df['ASIN'] = input_df['ASIN'].apply(lambda x: str(x).zfill(10))
        input_df.drop_duplicates(inplace=True)

        # Transform input offers as tuple
        pairs = []
        for index, row in input_df.iterrows():
            pairs.append((row['ASIN'], row['Source'], row['Target']))

        # Run ada command in background to keep authentication fresh
        # subprocess.call("authenticate_status_role.sh", shell=True)
        cmd = '''ada credentials update 1234'''
        subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        time.sleep(20)

        # Clear previous output and write column names
        init_line = 'ASIN,Source,Target,merchant_type,SourceBuyable,TargetBuyable,Yank,Overlap,' \
                    'GS Restriction - Prime Exclusive,GS Restriction - TOOS,Localization,aglExportability,retailExportability,Blocked'
        with open('result.txt', 'w', encoding='utf-8') as f:
            f.write(init_line + '\n')

        # print(pairs)
        start = time.time()
        p = Pool(processes=8)
        p_outputs = p.map(self.call, pairs)
        # pdb.set_trace()
        print(p_outputs)
        for p_output in p_outputs:
            # print(p_output)
            with open('result.txt', 'a', encoding='utf-8') as f:
                f.write(p_output[0] + '\n')
                f.write(p_output[1] + '\n')
        p.close()
        p.join()

        end = time.time()
        print(end-start)

        '''

        # for test
        for pair in pairs:
            self.call(pair)
        '''
        output_df = pd.read_csv('result.txt', dtype=object,sep=',')
        # 这里需要确认
        # output_df['GS Restriction - TOOS'] = '[Record Not Found]'
        # output_df['GS Restriction - Add-On'] = '[Record Not Found]'
        # output_df['Syndicated'] = '[YES]'
        output_df['Exportability'] = output_df.apply(lambda x: self.get_exportability(x),axis = 1)


        # output_df.to_excel('api_result.xlsx', index=False)
        output_df['GS Restriction - Add-On'] = '[Record Not Found]'
        output_df['Syndicated'] = '[YES]'
        output_df['Overlap'] = output_df['Overlap'].apply(lambda x:'[No Overlap]' if x=='[NO]' else
        ('YES' if x=='[Has Overlap]' else '[Record Not Found]'))
        # output_df_final = output_df[['ASIN','Source','Target','merchant_type','SourceBuyable','Overlap',
        #                              'GS Restriction - Prime Exclusive','GS Restriction - TOOS',
                                     # 'GS Restriction - Add-On','Syndicated','Blocked']]

        output_df_final = output_df[
            ['ASIN', 'Source', 'Target', 'merchant_type', 'SourceBuyable', 'Overlap',
             'GS Restriction - Prime Exclusive', 'GS Restriction - TOOS',
             'GS Restriction - Add-On', 'Syndicated', 'Blocked']]
        output_df_final.to_excel('result.xlsx', index=False)
        print(output_df_final)
        print(output_df)
        return output_df_final

if __name__ == '__main__':
    input('please first run "mwinit" in cmd, press Enter to continue')
    print('Processing....')
    input_df = pd.read_excel(r'input.xlsx')
    fetch_from_API().api_main(input_df)
# asin = 'B0000C8TGH'
# source_marketplace_id = '1'
# target_marketplace_id = '3240'
#
# cmd = '''awscurl -H "Content-Type: application/x-amz-json-1.1" "https://t9v1xe6igg.execute-api.us-east-1.amazonaws.com/prod/status?sourceMarketplaceId=%s&targetMarketplaceId=%s&asins=%s&version=2"''' % (
# str(source_marketplace_id), str(target_marketplace_id), str(asin))
# p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
# status_payload = p.communicate()
# status_json = json.loads(json.loads(status_payload[0])['result'][0])