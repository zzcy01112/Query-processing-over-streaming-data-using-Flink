from datetime import date


class Lineitem:
    """
    Lineitem类，保留Q3中使用的字段
    """
    def __init__(self, l_orderkey: int, l_extendedprice: float, l_discount: float, l_shipdate: date):
        self.l_orderkey = l_orderkey
        self.l_extendedprice = l_extendedprice
        self.l_discount = l_discount
        self.l_shipdate = l_shipdate
