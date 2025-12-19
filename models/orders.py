from datetime import date


class Orders:
    """
    Orders类，保留Q3中使用的字段
    """
    def __init__(self, o_orderkey: int, o_custkey: int, o_orderdate: date, o_shippriority: int):
        self.o_orderkey = o_orderkey
        self.o_custkey = o_custkey
        self.o_orderdate = o_orderdate
        self.o_shippriority = o_shippriority
