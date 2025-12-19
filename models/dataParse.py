"""
定义一些数据解析相关的常量
"""

class Operator:
    """
    操作类型
    """
    INSERT = '+'
    DELETE = '-'

class Relation:
    """
    关系类型
    """
    CUSTOMER = 'CU'
    ORDERS = 'OR'
    LINEITEM = 'LI'

class Index:
    """
    字段索引，offset=1 (0号位置为操作符+关系类型)
    """
    OP_REL = 0
    C_CUSTKEY = 1
    C_MKTSEGMENT = 7
    O_ORDERKEY = 1
    O_CUSTKEY = 2
    O_ORDERDATE = 5
    O_SHIPPRIORITY = 8
    L_ORDERKEY = 1
    L_EXTENDEDPRICE = 6
    L_DISCOUNT = 7
    L_SHIPDATE = 11

class Symbol:
    """
    +/- 符号 -> +1 / -1
    """
    SYMBOL_MAP = {
        '+': 1,
        '-': -1
    }
