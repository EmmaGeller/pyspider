import psycopg2
import six
import yaml

class PostGres():
   def __init__(self):
       f=open('setting.yaml')
       self.setting=yaml.load(f)['postgres']
       print(self.setting)
       # 连接数据库
       self.connect = psycopg2.connect(
           host=self.setting['host'],
           port=self.setting['port'],
           dbname=self.setting['dbname'],
           user=self.setting['user'],
           password=self.setting['password']
       )
       self.cursor = self.connect.cursor();
       print('success')

   def save_item(self, item):
       global sql
       try:
           table_name = item.pop('table_name')
           abs_id = item.pop('abs_id')
           col_str = ''
           row_str = ''
           for key in item.keys():
               col_str = col_str + " " + key + ","
               row_str = "{}'{}',".format(row_str,
                                          item[key] if "'" not in item[key] else item[key].replace("'", "\\'"))
           if abs_id != None:
               sql = "insert INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET ".format(table_name,
                                                                                              col_str[1:-1],
                                                                                              row_str[:-1],
                                                                                              abs_id)
               for (key, value) in six.iteritems(item):
                   sql += "{} = '{}', ".format(key, value if "'" not in value else value.replace("'", "\\'"))
               sql = sql[:-2]
           else:
               sql = "insert INTO {} ({}) VALUES ({})".format(table_name,
                                                              col_str[1:-1],
                                                              row_str[:-1])
           self.cursor.execute(sql)
       except psycopg2.InterfaceError as exc:
           # 再次尝试连接数据库
           self.connect = psycopg2.connect(
               host=self.setting['host'],
               port=self.setting['port'],
               dbname=self.setting['dbname'],
               user=self.setting['user'],
               password=self.setting['password']
           )
           self.cursor = self.connect.cursor();
           self.cursor.execute(sql)
       except Exception as e:
           raise e
       self.connect.commit()
