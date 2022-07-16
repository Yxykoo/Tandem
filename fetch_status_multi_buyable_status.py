import json
import pandas as pd
import os
import subprocess
import time 
# from status_data_processor import *
from multiprocessing import Process, Pool
import pdb
class fetch_from_destinationbuyable_API(object):
    def __init__(self):
        self.line_list = []

    def call(self, pairs):
        success = False
        retry = 0
        while not success:
            try:
                asin = pairs[0]
                source_marketplace_id = pairs[1]
                target_marketplace_id = pairs[2]
                # print(source_marketplace_id,target_marketplace_id)
                cmd = '''awscurl -H "Content-Type: application/x-amz-json-1.1" "http://sable1234"''' % (str(asin),str(source_marketplace_id))
                p = subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                status_payload = p.communicate()
                # print(status_payload[0].decode('utf-8','ignore'))
                # status_json = json.loads(json.loads(status_payload[0])['result'][0])
                # line_R, line_F = StatusDataProcessor(status_json).apply()
                #with open('result.txt', 'a', encoding='utf-8') as f:
                #    f.write(line_R + '\n')
                #    f.write(line_F + '\n')

                success = True
            except Exception as exc:
                # print("Error fetching status payload for ASIN {}".format(asin), str(exc))
                retry += 1
                if retry == 10:
                    print('Retried 10 times:', str(asin), str(source_marketplace_id), str(target_marketplace_id), str(exc))
                    return ','.join([str(asin),str(source_marketplace_id),str(target_marketplace_id),b''])
                    break
        # return line_R, line_F
        return [str(asin),str(source_marketplace_id),str(target_marketplace_id),status_payload[0]]

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
        cmd = '''ada credentials update --account=31234 &'''
        subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        time.sleep(20)

        # Clear previous output and write column names
        # init_line = 'ASIN,Source,Target,merchant_type,SourceBuyable,Overlap,GS Restriction - Prime Exclusive'
        # with open('result.txt', 'w', encoding='utf-8') as f:
        #     f.write(init_line + '\n')

        start = time.time()
        p = Pool(processes=5    )
        p_outputs = p.map(self.call, pairs)
        # for p_output in p_outputs:
        #     with open('result.txt', 'a', encoding='utf-8') as f:
        #         f.write(p_output[0] + '\n')
        #         f.write(p_output[1] + '\n')

        p.close()
        p.join()
        print(p_outputs)

        df = pd.DataFrame(columns=['ASIN','Source','Target','Buyable in destination'],data=p_outputs)
        # df.to_excel('TargetBuyable_test.xlsx', index=False)
        # pdb.set_trace()
        # pdb.set_trace()
        df['Buyable in destination'] = df['Buyable in destination'].map(lambda x:'[NO]' if '\x10' in x.decode('utf-8') else '[YES]')
        # code_source_mapping = {'4':'DE','3':'UK','1':'US','6':'JP'}
        # df['Source'] = df['Source'].map(lambda x:code_source_mapping[x])
        # df['Target'] = 'CN'
        df.to_excel('TargetBuyable.xlsx',index=False)


if __name__ == '__main__':
    input_df = pd.read_excel(r'input.xlsx')
    fetch_from_API().api_main(input_df)
