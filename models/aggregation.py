from datetime import date


class Aggregation:
    """
    聚合层，内容为group_by中的字段和revenue
    """
    def __init__(self, l_orderkey: int, o_orderdate: date, o_shippriority: int, revenue: float):
        self.l_orderkey = l_orderkey
        self.o_orderdate = o_orderdate
        self.o_shippriority = o_shippriority
        self.revenue = revenue
