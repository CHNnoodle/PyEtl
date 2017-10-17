# coding:utf8

import logging
import copy
import cx_Oracle
import os
import commands
import hdfs
from sqlalchemy import *
from PySynInfo import getdns


# 插入日志
def p_insert_log(d_info):
    db = d_info.get('meta_db')
    conn = db.raw_connection()
    cursor = conn.cursor()
    out_proc_num = cursor.var(cx_Oracle.NUMBER)
    args = [d_info.get('acctday'), d_info.get('target_proc'), d_info.get('syn_type'), out_proc_num]
    try:
        cursor.callproc('xijia.p_insert_log', args)
        d_info['proc_num'] = out_proc_num.getvalue()
        return d_info

    except Exception, etp:
        raise Exception(etp)
    finally:
        cursor.close()


# 更新日志
def p_update_log(d_info):
    db = d_info.get('meta_db')
    conn = db.raw_connection()
    args = [d_info.get('proc_num'), d_info.get('finish_flag'), d_info.get('rowcount'), d_info.get('retcode'),
            str(d_info.get('retinfo'))]
    cursor = conn.cursor()
    try:
        cursor = conn.cursor()
        cursor.callproc('xijia.p_update_log', args)
    except Exception, etp:
        raise Exception(etp)
    finally:
        cursor.close()


def procedure(d_info):
    meta_db = d_info.get('meta_db')
    meta_conn = meta_db.raw_connection()
    meta_cursor = meta_conn.cursor()
    try:
        out_retcode = meta_cursor.var(cx_Oracle.STRING)
        out_retinfo = meta_cursor.var(cx_Oracle.STRING)
        procname = d_info.get('target_proc')
        acctday = d_info.get('acctday')
        syn_type = d_info.get('syn_type')
        args = [acctday, syn_type, out_retcode, out_retinfo]
        meta_cursor.callproc(procname, args)
        d_info['retcode'] = out_retcode.getvalue()
        d_info['retinfo'] = out_retinfo.getvalue()
        logging.info('{target_proc}({acctday}): {retcode}'.format(**d_info))
    except Exception, etp:
        raise Exception(etp)
    finally:
        meta_cursor.close()


def table_to_table(d_info):
    d_info = p_insert_log(d_info)
    try:
        d_info['source_dns'] = getdns(d_info.get('source_dbname').upper())
        source_db = create_engine(d_info.get('source_dns'))
        source_conn = source_db.connect()
        try:
            logging.info('开始从源库{source_dbname}抽取数据'.format(**d_info))
            seldata = source_conn.execute(text(d_info.get('sql_select').format(**d_info)))
            d_info['target_dns'] = getdns(d_info.get('target_dbname').upper())
            target_db = create_engine(d_info.get('target_dns'))
            target_conn = target_db.connect()
            try:
                trans = target_conn.begin()
                try:
                    target_conn.execute(text(d_info.get('sql_delete').format(**d_info)))
                    insdata = target_conn.execute(text(d_info.get('sql_insert')), list(seldata))
                    d_info['rowcount'] = insdata.rowcount
                    trans.commit()
                    logging.info(('插入目标表{target_proc}--%s行数据' % insdata.rowcount).format(**d_info))
                    d_info['finish_flag'] = 'finish'
                    d_info['retcode'] = 'success'
                except Exception, etp:
                    trans.rollback()
                    logging.error('错误！回滚清除的数据')
                    raise Exception(etp)
            except Exception, etp:
                raise Exception(etp)
            finally:
                target_conn.close()
        except Exception, etp:
            raise Exception(etp)
        finally:
            source_conn.close()
    except Exception, etp:
        d_info['finish_flag'] = 'break'
        d_info['retcode'] = 'fail'
        d_info['retinfo'] = str(etp)
        logging.error(etp)

    p_update_log(d_info)
    logging.info('{target_proc}({acctday}): {retcode}'.format(**d_info))


def table_to_hdfs(d_info):
    d_info = p_insert_log(d_info)
    localtempfile = str(d_info.get('localpath')) + str(d_info.get('source_proc')) + '.tmp'
    localfile = str(d_info.get('localpath')) + str(d_info.get('source_proc')) + '.lz4'
    if d_info.get('syn_strategy') == 1:
        hdfspath = str(d_info.get('target_proc'))
    else:
        hdfspath = str(d_info.get('target_proc')) + str(d_info.get('acctday')) + '/'

    try:
        d_info['source_dns'] = getdns(d_info.get('source_dbname').upper())
        source_db = create_engine(d_info.get('source_dns'))
        source_conn = source_db.connect()
        try:
            seldata = list(source_conn.execute(text(d_info.get('sql_select').format(**d_info))))
        except Exception, etp:
            logging.error(etp)
            raise Exception(etp)
        finally:
            source_conn.close()

        with open(localtempfile, 'wb+') as f:
            for row_data in seldata:
                f.write(''.join(row_data).encode('utf-8') + '\n')

        logging.info('数据已写入本地，开始压缩数据')
        oscmd = 'lz4 ' + localtempfile + ' ' + localfile
        (status, output) = commands.getstatusoutput(oscmd)
        if status == 0:
            try:
                d_info['target_dns'] = getdns(d_info.get('target_dbname').upper())
                client = hdfs.Client(d_info.get('target_dns'), root="/", timeout=100, session=False)
                if not client.status(hdfspath, strict=False):
                    client.makedirs(hdfspath, permission=777)
                print localfile, hdfspath
                client.upload(hdfspath, localfile)
                d_info['finish_flag'] = 'finish'
                d_info['retcode'] = 'success'
            except Exception, etp:
                logging.error(etp)
                raise Exception(etp)
        else:
            raise Exception('lz4压缩失败')

    except Exception, etp:
        d_info['finish_flag'] = 'break'
        d_info['retcode'] = 'fail'
        d_info['retinfo'] = etp
        logging.error(etp)
    finally:
        os.system('rm -f ' + localtempfile + ' ' + localfile)
        logging.info('删除本地文件')

    p_update_log(d_info)
    logging.info('{target_proc}({acctday}): {retcode}'.format(**d_info))


def table_to_es(d_info):
    print d_info
    pass


def syn_proc(in_info, instr):
    d_info = copy.deepcopy(in_info)
    d_info['meta_db'] = create_engine(d_info.get('meta_dns'))
    try:
        strs = instr.split('|*|')
        if len(strs) == 13:
            d_info['acctday'] = strs[0]
            d_info['target_proc'] = strs[1]
            d_info['syn_method'] = strs[2]
            d_info['syn_type'] = strs[3]
            d_info['target_db'] = strs[4]
            d_info['target_dbname'] = strs[5]
            d_info['source_db'] = strs[6]
            d_info['source_dbname'] = strs[7]
            d_info['sql_select'] = strs[8]
            d_info['sql_delete'] = strs[9]
            d_info['sql_insert'] = strs[10]
            d_info['source_proc'] = strs[11]
            d_info['syn_strategy'] = strs[12]

            syn_method = d_info.get('syn_method')

            if syn_method == '1':
                logging.info('执行过程 {target_proc},同步类型 procedure'.format(**d_info))
                procedure(d_info)
            elif syn_method == '2':
                logging.info('执行过程 {target_proc},同步类型 table_to_table'.format(**d_info))
                table_to_table(d_info)
            elif syn_method == '3':
                logging.info('执行过程 {target_proc},同步类型 table_to_hdfs'.format(**d_info))
                table_to_hdfs(d_info)
            elif syn_method == '4':
                logging.info('执行过程 {target_proc},同步类型 table_to_es'.format(**d_info))
                table_to_es(d_info)
        else:
            raise Exception('fail', '参数不对，终止')

    except Exception, etp:
        logging.error(etp)
        raise Exception('fail', etp)


if __name__ == '__main__':
    try:
        pass
    except Exception, e:
        print e
