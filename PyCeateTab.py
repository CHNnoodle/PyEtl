# coding:utf8

import os
from PyLog import getdns
from sqlalchemy import *


def syntable(d_info):
    source_db = d_info.get('source_db').lower()
    if source_db == 'mysql':
        sql = """SELECT
  concat(column_name,' ',
  case 
    when numeric_precision is not null then concat('number(',numeric_precision,',',numeric_scale,')')
    when data_type in ('date','datetime','timestamp') then 'date'
    when data_type in ('text','varchar') and character_maximum_length*2>4000 then 'varchar2(3999)' 
    else concat('varchar2(',ifnull(character_maximum_length*2,1000),')') end) xsql
FROM
  information_schema.columns t
where concat(table_schema,'.',table_name) = '{source_table}'
order by ordinal_position""".format(**d_info)
    elif source_db == 'sqlserver':
        sql = """select t1.name+' '+
case when t1.length>=2000 then 'varchar2(3999)'
when t2.variable = 'true' then 'varchar2('+ convert(varchar,t1.length*2)+')'
when t2.name like '%time%' then 'varchar2(30)'
else 'number('+convert(varchar,t1.xprec)+','+convert(varchar,t1.xscale)+')' end colsql
from syscolumns t1, systypes t2
where t1.xusertype = t2.xusertype 
and t1.id = object_id('{source_table}')
order by colid""".format(**d_info)
    elif source_db == 'oracle':
        sql = ''

    print sql

    sdb = create_engine(d_info.get('source_dns'))
    sconn = sdb.connect()
    try:
        result_set = sconn.execute(sql)
        tabCreateScript = 'create table {target_table} (\n'
        for col in result_set:
            tabCreateScript = tabCreateScript + col[0] + ',\n'
        tabCreateScript = tabCreateScript[:-2] + ')\n'
        in_dic['tabCreateScript'] = tabCreateScript.format(**d_info)
        print in_dic

    except Exception, e:
        print e
        raise Exception(e)
    finally:
        sconn.close()

    tdb = create_engine(d_info.get('target_dns'))
    tconn = tdb.connect()
    try:
        tconn.execute("call xijia.pkg_etl_model.p_droptable_ifexists('{target_table}')".format(**d_info))
        tconn.execute(d_info.get('tabCreateScript'))

    except Exception, e:
        print e
        raise Exception(e)
    finally:
        tconn.close()


if __name__ == '__main__':
    os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
    # 输入参数
    in_dic = {
        'source_db': 'mysql',
        'source_dbname': 'mysql1',
        'source_table': 'tsinghua_data_monitor.format_tasks',
        'target_db': 'oracle',
        'target_dbname': 'ORACLE1',
        'target_table': 'xijia.format_tasks'
    }

    sourcedns = getdns(in_dic.get('source_dbname').upper())
    in_dic['source_dns'] = sourcedns
    print sourcedns

    targetdns = getdns(in_dic.get('target_dbname').upper())
    in_dic['target_dns'] = targetdns
    print targetdns

    syntable(in_dic)
