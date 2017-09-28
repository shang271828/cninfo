# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CninfoItem(scrapy.Item):
    # define the fields for your item here like:
    
    sourceType = scrapy.Field()
    file_path = scrapy.Field()
    adjunctSize = scrapy.Field()
    adjunctType = scrapy.Field()
    adjunctUrl = scrapy.Field()
    announcementContent = scrapy.Field()
    announcementId = scrapy.Field()
    announcementTime = scrapy.Field()
    announcementTitle = scrapy.Field()
    announcementType = scrapy.Field()
    announcementTypeName = scrapy.Field()
    associateAnnouncement = scrapy.Field()
    batchNum = scrapy.Field()
    columnId = scrapy.Field()
    important = scrapy.Field()
    orgId = scrapy.Field()
    pageColumn = scrapy.Field()
    secCode = scrapy.Field()
    secName = scrapy.Field()
    storageTime = scrapy.Field()
