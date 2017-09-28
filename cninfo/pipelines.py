#coding:utf-8
import mysql.connector
import logging
  
class MySQLStorePipeline(object):
    tableName = "cninfo_realtime";
    
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(
            user='root',
            password='hoboom',
            host='106.75.3.227',
            database='scrapy',
            charset='utf8'
            )
            self.cur = self.conn.cursor(buffered=True)
        except Exception as e:
            logging.error(e, exc_info=True)
            raise
        
    def __del__(self):    
        self.cur.close()    
        self.conn.close()  
             
    def process_item(self, item, spider): 
        try:
#             self.cur.execute("SELECT count(*) FROM "+self.tableName+" WHERE seq = "+item['seq'])
#             exists = self.cur.fetchone()[0]
#             if exists == 0:
            self.insert_data(item, self.tableName)
#             else:
#                 print "exists!"
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.error("error from question: ", item['question'])
            raise
        return item
    
        
    def insert_data(self, item, table):  
        insertSql = """insert into """+table+""" (%s) values ( %s )""" 
          
        keys = item.fields.keys()    
        fields = u','.join(keys)    
        qm = u','.join([u'%s'] * len(keys))    
        sql = insertSql % (fields, qm)    
        data = [item[k] for k in keys]
        try:
            self.cur.execute(sql, data)
            self.conn.commit()
        except Exception as e:
            logging.error(e, exc_info=True)
            self.conn.rollback()
            raise