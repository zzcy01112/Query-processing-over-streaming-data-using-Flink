from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from models.aggregation import Aggregation
from models.relationJoin import RelationJoin
from modules.logger_setup import logger


class AggregationProcess(KeyedProcessFunction):
    """
    处理聚合逻辑
    以group by 的列为聚合主键 (l_orderkey, o_orderdate, o_shippriority)
    接收来自 RelationJoin 的 revenue 增量，revenue_delta = symbol(+/-) * l_extendedprice * (1 - l_discount)
    维护当前的 revenue 状态，将结果 (Aggregation) 输出到下游
    """
    def open(self, runtime_context: RuntimeContext):
        """
        初始化 revenue_state 状态存储
        :param runtime_context: 运行时上下文
        :return: None
        """
        # 记录当前聚合的 revenue
        self.revenue = runtime_context.get_state(
            ValueStateDescriptor('revenue_state', Types.FLOAT()))

    def process_element(self, value: RelationJoin, ctx: 'KeyedProcessFunction.Context'):
        """
        处理每个 RelationJoin 事件，更新 revenue 并输出结果
        :param value: RelationJoin 对象
        :param ctx: 上下文
        :return: None
        """
        # 计算 revenue 增量并更新状态
        symbol, order, lineitem = value.symbol, value.order, value.lineitem
        revenue_delta = symbol * lineitem.l_extendedprice * (1 - lineitem.l_discount)
        new_revenue = (self.revenue.value() or 0.0) + revenue_delta
        self.revenue.update(new_revenue)
        logger.info(f"更新聚合结果: l_orderkey={lineitem.l_orderkey}, o_orderdate={order.o_orderdate}, "
                     f"o_shippriority={order.o_shippriority}, revenue_delta={revenue_delta}, new_revenue={new_revenue}")
        # 输出当前聚合结果
        yield Aggregation(lineitem.l_orderkey, order.o_orderdate, order.o_shippriority, new_revenue)
