import pandas as pd
import win32com.client as win32
import datetime
import time
import os
import pdb
import getpass
import shutil
import sys
from zipfile import ZipFile
import urllib.request
from bs4 import BeautifulSoup
class HandleOutlook:
    def __init__(self):
        self.outlook = win32.Dispatch('Outlook.Application').GetNamespace("MAPI")
        self.user = getpass.getuser()
        self.last_time=time.time()
        self.is_inflow_count_same_flag = 0
        self.last_inflow_count = 0
        self.now_time = time.time()
        self.email_time_span = 7
    def download_excel_from_outlook(self,idinfo_list):
        #找到根目录
        root_folder = self.outlook.Folders[self.user+'@amazon.com']
        #用Folders找subfolder
        bulk_folder = root_folder.Folders['bulk_output']
        date_time = datetime.datetime.now()
        LastTwohoursDateTime = date_time - datetime.timedelta(hours=self.email_time_span)
        #过滤一下近两小时来的bulk结果
        file_path = []
        #判断id有没有发到邮件里，等id都到齐了，再去读下载文件
        new_idinfo_list=[]
        while True:
            inflow_count = 0
            for item in bulk_folder.Items.Restrict("[ReceivedTime] >= '"+LastTwohoursDateTime.strftime('%m/%d/%Y %H:%M %p') +"'"):
                for id in idinfo_list:
                    if id[33:] in item.Subject:
                        inflow_count+=1
                    else:
                        continue

            if (inflow_count == len(idinfo_list)) or (self.is_inflow_count_same_flag==1 and ((self.now_time-self.last_time)>3900)):
                #如果提交的bulk文件数量等于收到的邮件数量，也就是bulk结果数量正常，则读取所有bulk结果
                if (inflow_count == len(idinfo_list)):
                    break
                #如果提交的bulk文件数量大于收到的邮件数量，且等待时间大于15分钟，也就是bulk结果数量小于应收到的邮件数量，则保存现收到的bulk结果，舍弃未收到的结果
                else:
                    for item in bulk_folder.Items.Restrict("[ReceivedTime] >= '" + LastTwohoursDateTime.strftime('%m/%d/%Y %H:%M %p') + "'"):
                        for id in idinfo_list:
                            if id[33:] in item.Subject:
                                new_idinfo_list.append(id)
                    idinfo_list = new_idinfo_list
                    print('舍弃了bulk结果')
                    break
            #在while循环里面加一个计时器，记录本次inflow_count和上次的last_inflow_count数字，上次的时间last_time，当inflow_count==last_inflow_count的时间now_time，以及通过is_inflow_count_same_flag记录asin count是否一直一样
            #首次asin count一样的话is_inflow_count_same_flag=0，后面asin count不变的话记录为1
            else:
                if self.last_inflow_count == inflow_count:
                    if self.is_inflow_count_same_flag == 1:
                        self.now_time = time.time()
                    else:
                        self.last_time = time.time()
                        self.is_inflow_count_same_flag = 1
                else:
                    self.is_inflow_count_same_flag = 0

            self.last_inflow_count = inflow_count



        for item in bulk_folder.Items.Restrict("[ReceivedTime] >= '"+LastTwohoursDateTime.strftime('%m/%d/%Y %H:%M %p') +"'"):
            item.Unread = False
            for request_id in idinfo_list:
                if request_id[33:] in item.Subject:
                    #下载附件,用beautifulsoup来组织一下body的格式，方便抓取下载链接
                    email_text = item.HTMLBody
                    soup = BeautifulSoup(email_text,features='lxml')
                    download_link = soup.find_all('li')[1].text[13:]
                    urllib.request.urlretrieve(download_link,os.getcwd()+r'/'+request_id[0:5]+'_'+request_id[33:]+'.csv')
                    file_path.append(os.getcwd()+r'/'+request_id[0:5]+'_'+request_id[33:]+'.csv')
        return file_path

    def download_oculus_inflow_from_outlook(self):
        oculus_inflow_path = os.getcwd() + '/Oculus_inflow'
        if os.path.exists(oculus_inflow_path):
            shutil.rmtree(oculus_inflow_path)
        user = getpass.getuser()
        outlook = win32.Dispatch('Outlook.Application').GetNamespace("MAPI")
        root_folder = outlook.Folders[user + '@amazon.com']
        oculus_report_folder = root_folder.Folders['oculus_report']
        date_time = datetime.datetime.now()
        if not os.path.exists(oculus_inflow_path):
            os.mkdir(oculus_inflow_path)
        oculus_inflow_count = 0
        for item in oculus_report_folder.Items.Restrict("[ReceivedTime] >= '" + date_time.strftime('%m/%d/%Y') + "'"):
            if (item.Subject == 'Oculus Ad-hoc Dryrun') and (item.Attachments.Count > 0):
                # pdb.set_trace()
                item.Attachments[0].SaveAsFile(os.path.join(oculus_inflow_path, str(item)))
                oculus_inflow_count+=1
        if oculus_inflow_count == 0:
            print('未找到当日的oculus inflow')
            sys.exit()
        file_path = oculus_inflow_path + '\\' + os.listdir(oculus_inflow_path)[0]
        # print(file_path)
        with ZipFile(file_path, 'r') as zip:
            # zip.printdir()
            zip.extractall(os.getcwd() + '\Oculus_inflow')
            oculus_file_name = zip.namelist()[0]
        oculus_file_path = os.path.join(oculus_inflow_path, oculus_file_name)
        return oculus_file_path

if __name__ == '__main__':
    handleoutlook = HandleOutlook()
    root_folder = handleoutlook.outlook.Folders[handleoutlook.user+'@amazon.com']
    print(root_folder.Name)
    for folder in root_folder.Folders:
        print(folder.Name)
    print(root_folder.Folders['bulk_output'].Name)