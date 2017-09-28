# -*- coding: UTF-8 -*-
import scrapy
import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.schema import Column
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.sqltypes import String
import logging
from scrapy.utils.log import configure_logging
from cninfo.items import CninfoItem
from scrapy.http.request.form import FormRequest
import re
import os
import requests
import calendar
import datetime

Base = declarative_base()
    
class Cninfo(Base):
    __tablename__ = 'cninfo_realtime'
    announcementId = Column(String, primary_key=True)

class CninfoBondSpider(scrapy.Spider):
    name = "cninfoBondSpider"
    engine = create_engine('mysql://root:hoboom@106.75.3.227:3306/scrapy?charset=utf8', echo=True, encoding='utf8')
    session = sessionmaker(bind=engine)()
    rootDirectory = "/data/dearMrLei/data/"
#     rootDirectory = "D:/data/dearMrLei/data/"
    
    def start_requests(self):
        # for update
        today = datetime.datetime.now()
        for year in range(today.year, 2009, -1):
            for month in range(12, 0, -1):
                if year == today.year and month > today.month:
                    continue
                day1, ndays = calendar.monthrange(year, month)
                yearString = str(year).zfill(4)
                monthString = str(month).zfill(2)
                if year==today.year and month==today.month:
                    ndays = today.day
                dayString = str(ndays).zfill(2)
                seDate = yearString+"-"+monthString+"-"+"01"+" ~ "+yearString+"-"+monthString+"-"+dayString
                formdata = {'column':'bond', 'tabName':'fulltext', 'seDate':seDate, 'pageNum':'1', 'pageSize':'50'}
                
                # 债券公告
                url = "http://www.cninfo.com.cn/cninfo-new/announcement/query"
                request_bond_latest = FormRequest(url=url, formdata=formdata, callback=self.parseData)
                request_bond_latest.meta['pageNum'] = 1
                request_bond_latest.meta['sourceType'] = 10
                request_bond_latest.meta['url'] = url
                request_bond_latest.meta['seDate'] = seDate
                yield request_bond_latest
        
        
    
    
    def parseData(self, response):
        try:
            result_json_string = response.body_as_unicode()
            sourceType = response.meta.get('sourceType')
            
            try:
                result_json = json.loads(result_json_string);
                announcements_json = result_json['classifiedAnnouncements']
            except:  # not available site
                return
            existCount = 0
            
            dataJsonList = []
            if announcements_json is None:
                if 'announcements' in result_json:
                    announcements_json = result_json['announcements']
                if announcements_json is not None:
                    for data_json in announcements_json:
                        dataJsonList.append(data_json)
            else:           
                for array_json in announcements_json:
                    for data_json in array_json:
                        dataJsonList.append(data_json)
            
            if len(dataJsonList) == 0:
                return
            
            for data_json in dataJsonList:
                dataItem = CninfoItem()
                self.fillItemByJson(data_json, dataItem)
                dataItem["sourceType"] = sourceType
                
                if self.session.query(Cninfo).filter(Cninfo.announcementId == dataItem['announcementId']).first():
                    existCount += 1
#                     if existCount >= 3:
#                         return
                else:
                    existCount = 0  # need continue 3
                    # download file
                    dataItem['file_path'] = None
                    yield dataItem
#                     if dataItem["adjunctUrl"] is not None:
#                         adjunctUrl = dataItem["adjunctUrl"]
#                         relative_path = self.downloadFile(adjunctUrl,sourceType)
#                         if relative_path is not None:
#                             dataItem['file_path'] = relative_path
#                             yield dataItem
            
            seDate = response.meta.get('seDate')
            pageNum = response.meta.get('pageNum')
            pageNum += 1
            formdata = {'column':'bond', 'tabName':'fulltext', 'seDate':seDate, 'pageNum':str(pageNum), 'pageSize':'50'}
            url = response.meta.get('url')
            request = FormRequest(url=url,
                    formdata=formdata, callback=self.parseData)
            request.meta['pageNum'] = pageNum
            request.meta['sourceType'] = sourceType
            request.meta['url'] = url
            yield request
            
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.error("Error process: " + response.url)
    
    def downloadFile(self, adjunctUrl,sourceType):
        attach_url = "http://www.cninfo.com.cn/" + adjunctUrl
        pubDate = "default"
        m = re.match(r".*?/(\d+)-(\d+)-(\d+)/.*?", adjunctUrl)
        if m:
            pubDate = m.group(1) + "/" + m.group(2) + "/" + m.group(3)
        
        if sourceType==10:
            directory = "bond/cninfo/" + pubDate
        else:
            directory = "cninfo/" + pubDate
        if not os.path.exists(self.rootDirectory + directory):
            os.makedirs(self.rootDirectory + directory)
            
        pdf_name = adjunctUrl.split("/")[-1]
        relative_path = directory + "/" + pdf_name
        pdf_path = self.rootDirectory + relative_path
        
#         if not os.path.isfile(pdf_path):
        pdfResponse = requests.get(attach_url, stream=True)
        if pdfResponse.status_code == 200:
            with open(pdf_path, "wb") as f:
                for chunk in pdfResponse.iter_content(chunk_size=1024): 
                    f.write(chunk)
            if os.path.getsize(pdf_path) > 0:
                return relative_path
        return None
    
    
    def fillItemByJson(self, indic_json, indicItem): 
        for key, value in indic_json.items():
            if key != "id":
                indicItem[key] = value

