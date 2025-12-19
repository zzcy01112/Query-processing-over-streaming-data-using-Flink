import datetime
from typing import Optional

from models.updateEvent import UpdateEvent
from models.orders import Orders
from models.customer import Customer
from models.lineitem import Lineitem
from models.dataParse import *


def read_file(file_path: str) -> list[str]:
    """
    读取文件内容，返回行列表
    :param file_path: 文件路径
    :return: 文件行列表
    """
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            return lines
    except FileNotFoundError:
        raise FileNotFoundError(f"{file_path} not found.")

def parse_line(line: str) -> Optional[list[str]]:
    """
    解析CSV格式的行，返回字段列表
    :param line: CSV格式的行字符串
    :return: 字段列表
    """
    if not line:
        return None
    fields = line.strip().split('|')
    return fields

def parse_date(date_str: str) -> datetime.date:
    """
    解析标准日期格式字符串(yyyy-mm-dd)，转换为date对象
    :param date_str: 日期字符串
    :return: datetime.date对象
    """
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}")

def parse_fields(fields: list[str]) -> Optional[UpdateEvent]:
    """
    解析字段列表，转换为适当的数据类型
    :param fields: 字段列表
    :return: 解析后的字段列表对应的数据类型
    """
    if not fields:
        return None

    event = None
    # 匹配字段
    op_rel = fields[Index.OP_REL]
    op, rel = op_rel[0], op_rel[1:3]

    if rel == Relation.ORDERS:
        o_orderkey = int(fields[Index.O_ORDERKEY])
        o_custkey = int(fields[Index.O_CUSTKEY])
        o_orderdate = parse_date(fields[Index.O_ORDERDATE])
        o_shippriority = int(fields[Index.O_SHIPPRIORITY])

        orders = Orders(o_orderkey, o_custkey, o_orderdate, o_shippriority)
        event = UpdateEvent(op, rel, orders)
    elif rel == Relation.CUSTOMER:
        c_custkey = int(fields[Index.C_CUSTKEY])
        c_mktsegment = fields[Index.C_MKTSEGMENT]

        customer = Customer(c_custkey, c_mktsegment)
        event = UpdateEvent(op, rel, customer)
    elif rel == Relation.LINEITEM:
        l_orderkey = int(fields[Index.L_ORDERKEY])
        l_extendedprice = float(fields[Index.L_EXTENDEDPRICE])
        l_discount = float(fields[Index.L_DISCOUNT])
        l_shipdate = parse_date(fields[Index.L_SHIPDATE])

        lineitem = Lineitem(l_orderkey, l_extendedprice, l_discount, l_shipdate)
        event = UpdateEvent(op, rel, lineitem)
    else:
        raise ValueError(f"Unknown relation type: {rel}")

    return event

