import datetime

from pyflink.common import Types
from pyflink.datastream import CoProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor

from models.dataParse import Operator
from models.updateEvent import UpdateEvent
from models.orders import Orders
from models.relationJoin import RelationJoin


class LineitemProcess(CoProcessFunction):
    """
    处理 Lineitem 表的流数据，处理来自 OrdersProcess 的存活状态通知
    按 l_orderkey 分组，记录所有属于同一 Order 的 Lineitem
    当 order alive 变化或 lineitem 插入/删除且满足 shipdate 筛选 (l_shipdate > 1995-03-15) 时，生成 RelationJoin 并向下游发送
    """

    def open(self, runtime_context: RuntimeContext):
        """
        初始化 lineitem_state 状态存储
        :param runtime_context: 运行时上下文
        :return: None
        """
        # 记录当前 Order 是否存活
        self.ord_alive = runtime_context.get_state(
            ValueStateDescriptor('ord_alive', Types.BOOLEAN()))
        # 记录属于同一 Order 的所有 Lineitem
        self.lineitems_by_order = runtime_context.get_list_state(
            ListStateDescriptor('lines_by_order', Types.PICKLED_BYTE_ARRAY()))
        # 当 order alive 时，缓存该 Order
        self.order_payload = runtime_context.get_state(
            ValueStateDescriptor('order_payload', Types.PICKLED_BYTE_ARRAY()))

    def process_element1(self, value: UpdateEvent, ctx: 'CoProcessFunction.Context'):
        """
        处理 Lineitem 更新事件
        :param value: UpdateEvent 对象
        :param ctx: 上下文
        :return: None
        """
        lineitem = value.payload

        if value.operator == Operator.INSERT:
            self.lineitems_by_order.add(lineitem)
        elif value.operator == Operator.DELETE:
            lines = list(self.lineitems_by_order.get()) or []
            lines = [
                l for l in lines
                if not (
                    l.l_orderkey == lineitem.l_orderkey
                    and l.l_shipdate == lineitem.l_shipdate
                    and l.l_extendedprice == lineitem.l_extendedprice
                    and l.l_discount == lineitem.l_discount
                )
            ]
            self.lineitems_by_order.update(lines)

        # 仅当 order 存活且 shipdate 满足条件时，发送 RelationJoin
        ord_alive = self.ord_alive.value() or False
        order = self.order_payload.value()
        if ord_alive and lineitem.l_shipdate > datetime.date(1995, 3, 15) and order is not None:
            if value.operator == Operator.INSERT:
                # logger.debug(f"发送 RelationJoin: symbol=+, l_orderkey={lineitem.l_orderkey}")
                yield RelationJoin("+", order, lineitem)
            elif value.operator == Operator.DELETE:
                # logger.debug(f"发送 RelationJoin: symbol=-, l_orderkey={lineitem.l_orderkey}")
                yield RelationJoin("-", order, lineitem)

    def process_element2(self, value: tuple[int, bool, Orders], ctx: 'CoProcessFunction.Context'):
        """
        处理来自 OrdersProcess 的 order alive 通知
        :param value: (c_orderkey, ord_alive, Orders) 元组
        :param ctx: 上下文
        :return: None
        """
        c_orderkey, ord_alive, order = value
        # logger.debug(f"处理 Orders 存活状态通知: o_orderkey={c_orderkey}, ord_alive={ord_alive}")
        # 更新 l_orderkey 对应的 Order 存活状态和 Order 缓存
        self.ord_alive.update(ord_alive)
        self.order_payload.update(order if ord_alive else None)
        # 遍历所有该 Order 关联的 Lineitem，发送 RelationJoin
        lines = list(self.lineitems_by_order.get()) or []
        for lineitem in lines:
            if lineitem.l_shipdate > datetime.date(1995, 3, 15):
                # logger.debug(f"发送 RelationJoin: symbol={'+' if ord_alive else '-'}, l_orderkey={lineitem.l_orderkey}")
                yield RelationJoin("+" if ord_alive else "-", order, lineitem)
