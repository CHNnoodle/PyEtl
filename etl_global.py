# coding:utf8


def _init():  # 初始化
    global _global_dict
    _global_dict = {}


def set_value(key, value):
    _global_dict[key] = value


def get_value(key, defValue=None):
    try:
        return _global_dict[key]
    except KeyError:
        return defValue

if __name__ == '__main__':
    print 'etl_global.py'
    _init()
    set_value('CODE', 'UTF-8')
    x = get_value('CODE')
    print x
