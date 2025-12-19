from models.dataParse import Symbol
from models.lineitem import Lineitem
from models.orders import Orders


class RelationJoin:
    """
    Orders与Lineitem关系的连接以及其增量
    """
    def __init__(self, symbol: str, order: Orders, lineitem: Lineitem):
        self.symbol = Symbol.SYMBOL_MAP[symbol]
        self.order = order
        self.lineitem = lineitem
