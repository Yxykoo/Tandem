
from Handle_inflow import *
from Handle_Outlook_modified import *
from Handle_BulkBlocker import *
from Elaine_check_chuanlian import *
from FindNoDPASINS import *
from fetch_status_multi_v2_chuanlian import fetch_from_API
from fetch_status_multi_buyable_status import fetch_from_destinationbuyable_API
from multiprocessing import Process
import pandas as pd
import buyable_api_WW
import pdb
import sys

def check_bulk_lifecycle_task(jump_over_bulk_flag,handle_inflow):
    if jump_over_bulk_flag == 'N':
        #跑buyability 输入的文件是input.xlsx，输出的文件是output.xlsx
        # buyable_api_WW.execution()
        #输入文件是output.xlsx，输出是Bulk_output.xlsx

        #这里会把buyable in destination=1的ASIN删掉，目前回把输入文件，也就是output.xlsx中的buyable in destination的ASIN拿出来
        # buyable_in_destination_asins_df = handle_inflow.get_buyable_in_destination_asins()
        #跑bulk

        input_df = pd.read_excel(handle_inflow.input_address)
        fetch_from_API().api_main(input_df)

    else:
        # handle_inflow = HandleInflow()
        # buyable_in_destination_asins_df = handle_inflow.get_buyable_in_destination_asins()
        pass

def check_targetbuyable_task(handle_inflow):
    # try:
    #     handle_inflow.change_input_file_format()
    # except:
    #     pass
    # input_df = pd.read_excel(handle_inflow.input_address)
    fetch_from_destinationbuyable_API().api_main(handle_inflow.inflow)
    # try:
    #     handle_inflow.change_back_input_file_format()
    # except:
    #     pass
def check_csi_task():
    findnodpasins = FindNoDPASINS()
    findnodpasins.execution()

# def buyable_in_destination_check(input_df):
#     source_cocde_mapping = {'US':'1','UK':'3','DE':'4','JP':'6','GB':'3'}
#     import requests
#     input_df['TargetBuyable'] = ''
#     #'\x01\x00\x10'---False; '\x01\x00\x11'---True
#     for index,row in input_df.itterows():
#         url = 'http://sable-responders-adhoc-pek.amazon.com/datapath/query/constellation/gfd/v2/isEligible/-/'+row['ASIN']+'?sourceMarketplaceId='+source_cocde_mapping[row['Source']]+'&targetCountryCode='+row['Target']+'&exportsProgram=GLOBAL_STORE&validationStack=TARGET&skipBlockList=true'
#         resp = requests.get(url)
#         buyable_in_destination_status = resp.content.decode('utf-8',errors='ignore')
#         if buyable_in_destination_status == '\x01\x00\x10':
#             input_df.loc[index,'TargetBuyable'] = 'False'
#         elif buyable_in_destination_status == '\x01\x00\x11':
#             input_df.loc[index,'TargetBuyable'] = 'True'
#     input_df.to_excel('targetbutable_status.xlsx',index=False)
if __name__ == '__main__':
    # handle_inflow = HandleInflow()
    # handlebulkblocker = HandleBulkBlocker(handle_inflow.inflow)
    # retail_inflow = handle_inflow.inflow

    #@todo construction
    task_type = str(input('请输入要执行的任务：1.活口; 2.oculus inflow; 3.SIM inflow; [1,2,3]: '))

    handle_inflow = HandleInflow()
    if task_type == '1':
        pass
    elif task_type == '2':
        outlook = HandleOutlook()
        oculus_file_path = outlook.download_oculus_inflow_from_outlook()
        handle_inflow.get_oculus_inflow(oculus_file_path)

    elif task_type == '3':

        handle_inflow.get_sim_inflow()
    else:
        print('输入了错误的数字，程序结束')
        sys.exit()

    #@todo 将source，target中的string国家转成数字，然后存到input.xlsx中

    #判断是否跳过bulk，如果否，则提交bulk文件，并且把id_list信息保存，如果是，则读取保存过的id_list（id_list用于在outlook中抓bulk的结果邮件）
    jump_over_bulk = input('是否要跳过bulk？请输入: [Y/N] ').upper()
    # check_bulk_lifecycle_task(jump_over_bulk,handle_inflow)
    #@todo 注掉

    try:
        handle_inflow.change_input_file_format()
    except:
        pass
    csi_process = Process(target=check_csi_task)
    csi_process.start()
    # check_csi_task()
    bulk_process = Process(target=check_bulk_lifecycle_task,args=(jump_over_bulk,handle_inflow))

    buyable_in_destination_process = Process(target=check_targetbuyable_task,args=(handle_inflow,))
    # 查CSI，读的是input.xlsx的ASIN
    bulk_process.start()
    buyable_in_destination_process.start()
    csi_process.join()
    buyable_in_destination_process.join()
    bulk_process.join()
    # pdb.set_trace()
    try:
        handle_inflow.change_back_input_file_format()
    except:
        pass
    bulk_result_part1 = pd.read_excel('result.xlsx')
    bulk_result_part2 = pd.read_excel('TargetBuyable.xlsx')
    # pdb.set_trace()
    bulk_inflow = bulk_result_part1.merge(bulk_result_part2,how='left',on=['ASIN','Source','Target'])

    bulk_inflow.to_excel('all_bulk_result.xlsx',index=False)

    bulk_inflow.rename(columns={'Target':'TargetMarketplace','Source':'SourceMarketplace','SourceBuyable':'Buyable in source'},inplace=True)
    # pdb.set_trace()
    # findnodpasins = FindNoDPASINS()
    # nodp_asin_info = findnodpasins.execution()
    # pdb.set_trace()
    #从outlook中下载bulk结果
    # handleoutlook = HandleOutlook()
    # file_path = handleoutlook.download_excel_from_outlook(id_list)
    # bulk_inflow = handle_inflow.concat_files(file_path)
    # bulk_inflow.to_excel('all_bulk_result.xlsx',index=False)
    #找retail的结果
    retail_inflow = handle_inflow.find_retail_inflow(bulk_inflow)
    #把buyability tool放到这里来跑，input是bulk output中的buyable in destination中的ASIN，output改成一个df
    retail_buyable_inflow = retail_inflow[retail_inflow['Buyable in destination']=='[YES]']
    retail_buyable_inflow = handle_inflow.convert_country_digit_to_str(retail_buyable_inflow)
    #如果bulk未查出buyable in destination的ASIN，则不去查buyability
    if retail_buyable_inflow.shape[0] != 0:
        buyable_api_WW.execution(retail_buyable_inflow)
    else:
        empty_df = pd.DataFrame([],columns=['ASIN','Source','Target','DestinationBuyable','SourceBuyable'])
        empty_df.to_excel('output.xlsx',index=False)
    # pdb.set_trace()
    buyable_in_destination_asins_df = pd.read_excel('output.xlsx')
    buyable_in_destination_asins_df = buyable_in_destination_asins_df[buyable_in_destination_asins_df['DestinationBuyable']==1]
    buyable_asins_list = buyable_in_destination_asins_df['ASIN'].tolist()
    buyable_asins_arc_list = handle_inflow.get_buyable_asin_arc_list(buyable_in_destination_asins_df)
    retail_inflow = handle_inflow.filter_buyable_inflow(buyable_asins_arc_list,retail_inflow)
    # retail_inflow.to_excel('filtered_retail_inflow.xlsx',index=False)
    # pdb.set_trace()
    # print(1)
    # print(retail_inflow)
    #处理bulk的blocker，输入是bulklifecycle中的retail记录
    handlebulkblocker = HandleBulkBlocker(retail_inflow)
    # pdb.set_trace()
    # print(2)
    #这里可以改成如果buyability跑出来的
    handlebulkblocker.find_boss_yanked(retail_buyable_inflow,buyable_in_destination_asins_df)
    # pdb.set_trace()
    # print(3)
    handlebulkblocker.find_nonbuyable_in_source()
    # pdb.set_trace()
    # print(4)
    handlebulkblocker.find_no_syndication()
    # pdb.set_trace()
    # print(5)
    handlebulkblocker.find_gs_restriction()
    # pdb.set_trace()
    # print(6)
    # handlebulkblocker.data['Blocked'] = handlebulkblocker.data['Blocked'].map(lambda x:'[Blocked]' if '[YES]' == str(x) else x)
    handlebulkblocker.find_blocked()
    # pdb.set_trace()
    # print(7)
    handlebulkblocker.find_buyable_in_destination(buyable_in_destination_asins_df)
    # pdb.set_trace()
    # print(8)
    handlebulkblocker.filtered_data.to_excel(os.getcwd()+'/'+'Bulk_output.xlsx',index=False)
    # pdb.set_trace()
    # print(9)
    #elaine的inflow用retail inflow
    #用Elaine查blocker，读的是Handle_inflow中的input
    # elaine = process_elaine('CN')
    # elaine_input_file = retail_inflow #这个inflow中有ASIN; Source; Target; Destinationbuyable; Sourcebuyable; ARC这几列
    elaine_input_file = handle_inflow.find_all_inflow(buyable_asins_arc_list) #这个inflow中有ASIN, Source, Target, DestinationBuyable, SourceBuyable
    # pdb.set_trace()
    elaine_result = elaine_process(handle_inflow.inflow)
    elaine_result['Source'] = elaine_result['Source'].map(lambda x:handlebulkblocker.source_scope[x])
    elaine_result['Target'] = elaine_result['Target'].map(lambda x: handlebulkblocker.destination_scope[x])
    nodp_asin_info = pd.read_excel('csi_no_dp_asins_result.xlsx')

    #连接Elaine和bulk和CSI的结果,full_asin_info中的ASIN全是带blocker的ASIN
    full_asin_info = pd.concat([handlebulkblocker.filtered_data,elaine_result,nodp_asin_info],ignore_index=True)
    # full_asin_info.to_excel('full_blocker.xlsx',index=False)
    # pdb.set_trace()
    #找unknown reason的ASIN
    handlebulkblocker.find_unknown_reason(retail_inflow,full_asin_info)

    full_asin_info = pd.concat([handlebulkblocker.filtered_data,elaine_result,nodp_asin_info],ignore_index=True)
    #改一下blocker名字
    full_asin_info = handlebulkblocker.change_nonbuyable_in_source_name(full_asin_info)
        #处理带有nonbuyable in source和其他blocker的asin，将nonbuyable in source的记录删除
    full_asin_info = handlebulkblocker.delete_nonbuyable_in_source(full_asin_info)
    # print(full_asin_info)
    full_asin_info['Source'] = full_asin_info['Source'].map(lambda x:handlebulkblocker.source_scope_back[x])
    full_asin_info['Target'] = full_asin_info['Target'].map(lambda x: handlebulkblocker.destination_scope_back[x])
    # full_asin_info['Target'] = full_asin_info['Target'].map(lambda x:handlebulkblocker.destination_scope_back[x])
    # pdb.set_trace()
    #把相同blocker name下的不同blocker reason整合到一个cell里，确保一个ASIN不会出现带有两个相同blocker name的记录

    full_asin_info = handlebulkblocker.blocker_reason_integration(full_asin_info)
    # print(full_asin_info)
    # pdb.set_trace()
    full_asin_info.to_excel(os.getcwd()+'/'+'Blocker_output.xlsx',index=False)





