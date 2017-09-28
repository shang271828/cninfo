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

Base = declarative_base()
    
class Cninfo(Base):
    __tablename__ = 'cninfo_realtime'
    announcementId = Column(String, primary_key=True)

class CninfoSpider(scrapy.Spider):
    name = "cninfoSpider"
    engine = create_engine('mysql://root:hoboom@106.75.3.227:3306/scrapy?charset=utf8', echo=True, encoding='utf8')
    session = sessionmaker(bind=engine)()
    rootDirectory = "/data/dearMrLei/data/"
    
    def start_requests(self):
        # for update
        formdata = {'pageNum':'1', 'pageSize':'50'}
        
        # 深市公告
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/szse_latest"
        request_szse = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_szse.meta['pageNum'] = 1
        request_szse.meta['sourceType'] = 1
        request_szse.meta['url'] = url
        yield request_szse
         
        # 沪市公告
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/sse_latest"
        request_sse = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_sse.meta['pageNum'] = 1
        request_sse.meta['sourceType'] = 2
        request_sse.meta['url'] = url
        yield request_sse
         
        # 两网公司及退市公司
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/staq_net_delisted_latest"
        request_staq_net_delisted = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_staq_net_delisted.meta['pageNum'] = 1
        request_staq_net_delisted.meta['sourceType'] = 3
        request_staq_net_delisted.meta['url'] = url
        yield request_staq_net_delisted
          
        # 股份转让系统挂牌公司
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/neeq_company_latest"
        request_neeq_company = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_neeq_company.meta['pageNum'] = 1
        request_neeq_company.meta['sourceType'] = 4
        request_neeq_company.meta['url'] = url
        yield request_neeq_company
         
        # 深市 投资者关系信息
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/szse_relation"
        request_szse_relation = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_szse_relation.meta['pageNum'] = 1
        request_szse_relation.meta['sourceType'] = 5
        request_szse_relation.meta['url'] = url
        yield request_szse_relation
         
        # 监管机构公告 - 深交所
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/regulator_szse_latest"
        request_regulator_szse = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_regulator_szse.meta['pageNum'] = 1
        request_regulator_szse.meta['sourceType'] = 6
        request_regulator_szse.meta['url'] = url
        yield request_regulator_szse
         
        # 监管机构公告 - 上交所
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/regulator_sse_latest"
        request_regulator_sse = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_regulator_sse.meta['pageNum'] = 1
        request_regulator_sse.meta['sourceType'] = 7
        request_regulator_sse.meta['url'] = url
        yield request_regulator_sse
         
        # 监管机构公告 - 证监会
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/regulator_zjh_latest"
        request_regulator_zjh = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_regulator_zjh.meta['pageNum'] = 1
        request_regulator_zjh.meta['sourceType'] = 8
        request_regulator_zjh.meta['url'] = url
        yield request_regulator_zjh
         
        # 预披露公告
        url = "http://www.cninfo.com.cn/cninfo-new/disclosure/pre_disclosure_latest"
        request_pre_disclosure = FormRequest(url=url, formdata=formdata, callback=self.parseData)
        request_pre_disclosure.meta['pageNum'] = 1
        request_pre_disclosure.meta['sourceType'] = 9
        request_pre_disclosure.meta['url'] = url
        yield request_pre_disclosure
        
        # 债券公告
#         url = "http://www.cninfo.com.cn/cninfo-new/disclosure/bond_latest"
#         request_bond_latest = FormRequest(url=url, formdata=formdata, callback=self.parseData)
#         request_bond_latest.meta['pageNum'] = 1
#         request_bond_latest.meta['sourceType'] = 10
#         request_bond_latest.meta['url'] = url
#         yield request_bond_latest
        
        
    
    
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
                dataItem["sourceType"] = response.meta.get('sourceType')
                
                if self.session.query(Cninfo).filter(Cninfo.announcementId == dataItem['announcementId']).first():
                    existCount += 1
                    if existCount >= 3:
                        return
                else:
                    existCount = 0  # need continue 3
                    # download file
                    if dataItem["adjunctUrl"] is not None:
                        adjunctUrl = dataItem["adjunctUrl"]
                        relative_path = self.downloadFile(adjunctUrl,sourceType)
                        if relative_path is not None:
                            dataItem['file_path'] = relative_path
                            yield dataItem
            
            pageNum = response.meta.get('pageNum')
            pageNum += 1
            formdata = {'pageNum':str(pageNum), 'pageSize':'50'}
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

