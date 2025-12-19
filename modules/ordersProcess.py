import datetime

from pyflink.common import Types
from pyflink.datastream import CoProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from models.dataParse import Operator
from models.updateEvent import UpdateEvent
from modules.logger_setup import logger


class OrdersProcess(CoProcessFunction):
    """
    处理 Orders 表的流数据，处理来自 CustomerProcess 的存活状态通知
    按 o_custkey 分组，记录所有属于同一 Customer 的 Orders
    维护每个 Order 的存活状态 (日期过滤: o_orderdate < '1995-03-15')
    当 Order 的存活状态变化时，向下游发送通知
    """

    def open(self, runtime_context: RuntimeContext):
        """
        初始化 orders_state 状态存储
        :param runtime_context: 运行时上下文
        :return: None
        """
        # 记录当前 Customer 是否存活
        self.cust_alive = runtime_context.get_state(ValueStateDescriptor('cust_alive', Types.BOOLEAN()))
        # 记录当前 Order 是否存活
        self.order_alive = runtime_context.get_map_state(
            MapStateDescriptor('order_alive', Types.INT(), Types.BOOLEAN()))
        # 记录属于同一 Customer 的所有 Orders
        self.orders_by_cust = runtime_context.get_map_state(
            MapStateDescriptor('orders_by_cust', Types.INT(), Types.PICKLED_BYTE_ARRAY()))

    def process_element1(self, value: UpdateEvent, ctx: 'CoProcessFunction.Context'):
        """
        处理 Orders 更新事件
        :param value: UpdateEvent 对象
        :param ctx: 上下文
        :return: None
        """
        order = value.payload
        # logger.debug(f"处理 Orders 更新事件: operator={value.operator}, o_orderkey={order.o_orderkey}, o_custkey={order.o_custkey}, o_orderdate={order.o_orderdate}")
        is_deleted = False
        # 判断插入/删除操作
        if value.operator == Operator.INSERT:
            # 添加 Order 到 orders_by_cust
            self.orders_by_cust.put(order.o_orderkey, order)
        elif value.operator == Operator.DELETE:
            # 从 orders_by_cust 移除 Order
            self.orders_by_cust.remove(order.o_orderkey)
            is_deleted = True

        # 更新 Order 存活状态
        customer_alive = self.cust_alive.value() or False
        order_alive = customer_alive and (order.o_orderdate < datetime.date(1995, 3, 15)) and not is_deleted
        order_alive = customer_alive and order_alive
        pre_order_alive = self.order_alive.get(order.o_orderkey) or False

        # 仅当存活状态变化时，发送通知
        if order_alive != pre_order_alive:
            self.order_alive.put(order.o_orderkey, order_alive)
            # logger.debug(f"Orders 存活状态变化: o_orderkey={order.o_orderkey}, order_alive={order_alive}")
            yield order.o_orderkey, order_alive, order

    def process_element2(self, value: tuple[int, bool], ctx: 'CoProcessFunction.Context'):
        c_custkey, customer_alive = value
        # logger.debug(f"处理 Customer 存活状态通知: c_custkey={c_custkey}, customer_alive={customer_alive}")
        # 更新 o_custkey 对应的 Customer 存活状态
        self.cust_alive.update(customer_alive)
        # 更新所有该 Customer 关联的 Orders 的存活状态
        orders = self.orders_by_cust.items()
        for o_orderkey, order in orders:
            pre_order_alive = self.order_alive.get(o_orderkey) or False
            order_alive = customer_alive and (order.o_orderdate < datetime.date(1995, 3, 15))  # 更新存活状态
            # 当存活状态变化时，发送通知
            if order_alive != pre_order_alive:
                self.order_alive.put(o_orderkey, order_alive)
                # logger.debug(f"Orders 存活状态变化: o_orderkey={o_orderkey}, order_alive={order_alive}")
                yield o_orderkey, order_alive, order
