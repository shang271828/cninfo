# coding:utf-8

from scrapy.http import FormRequest
import json 
from pandas.io.json import json_normalize
from scrapy.spiders import CrawlSpider
import urllib

class YJKBSpider(CrawlSpider):
    name = "yjkb"
    
    def start_requests(self):
        for i in range(1,6):
            yield FormRequest(url="http://www.cninfo.com.cn/cninfo-new/announcement/query",\
                                    formdata={"searchkey":u"业绩快报",\
                                              "columnTitle":u"历史公告查询",\
                                              "pageNum":str(i)},\
                              callback=self.parsePdfUrl)
    
    def parsePdfUrl(self, response):
        title=""
        result = response.xpath("//body/p/text()").extract_first()
        data = json.loads(result)
        df = json_normalize(data['announcements'])
#         self.logger.info('DF: %s',df)
        for i in range(len(df.index)):
            url = "http://www.cninfo.com.cn/" + df.loc[i]["adjunctUrl"]
            title ="["+df.loc[i]["secName"]+"]" + df.loc[i]["announcementTitle"]+".pdf"
            path = "report/"+title
            urllib.urlretrieve(url,path)
#             yield scrapy.Request(url, callback=self.parseSaveFile(response,title))
            