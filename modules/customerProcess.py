from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from models.dataParse import Operator
from models.updateEvent import UpdateEvent
from modules.logger_setup import logger


class CustomerProcess(KeyedProcessFunction):
    """
    处理 Customer 表的流数据
    判断是否满足筛选条件 (c_mktsegment == 'BUILDING')
    维护其存活状态
    当存活状态变化时，向OrdersProcess发送通知
    """
    def open(self, runtime_context: RuntimeContext):
        """
        初始化 customer_alive 状态存储
        :param runtime_context: 运行时上下文
        :return: None
        """
        self.alive = runtime_context.get_state(ValueStateDescriptor('customer_alive', Types.BOOLEAN()))

    def process_element(self, value: UpdateEvent, ctx: 'KeyedProcessFunction.Context'):
        """
        处理每个 Customer 更新事件
        :param value: UpdateEvent 对象
        :param ctx: 上下文对象
        :return: None
        """
        c_mktsegment = value.payload.c_mktsegment
        # logger.debug(f"处理 Customer 更新事件: operator={value.operator}, c_custkey={value.payload.c_custkey}, c_mktsegment={c_mktsegment}")
        # 插入操作
        if value.operator == Operator.INSERT and c_mktsegment == 'BUILDING':
            self.alive.update(True)
            # logger.debug(f"Customer {value.payload.c_custkey} 满足筛选条件，标记为存活.")
            # 发送存活通知 (c_custkey, customer_alive)
            yield value.payload.c_custkey, True
        # 删除操作
        elif value.operator == Operator.DELETE:
            self.alive.update(False)
            # 发送不活跃通知 (c_custkey, customer_alive)
            yield value.payload.c_custkey, False
