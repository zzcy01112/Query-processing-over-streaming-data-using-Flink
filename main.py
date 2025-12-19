from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

from modules.aggregationProcess import AggregationProcess
from modules.customerProcess import CustomerProcess
from modules.lineitemProcess import LineitemProcess
from modules.ordersProcess import OrdersProcess
from modules.parseCsvData import read_file, parse_fields, parse_line
from models.dataParse import Relation
from modules.logger_setup import logger

if __name__ == '__main__':
    """
    读取数据集，连接Flink流处理作业
    """
    # 创建Flink运行环境
    flink_env = StreamExecutionEnvironment.get_execution_environment()

    # 设置并行度
    flink_env.set_parallelism(1)

    # 读取数据集lines
    # logger.debug("Reading data from CSV file...")
    # lines = read_file('data/q3_output_1GB.csv')
    # logger.debug(f"Total lines read: {len(lines)}")
    #
    # # 将lines转换为Flink数据流
    data_stream = flink_env.read_text_file("data/q3_output_1GB.csv")
    logger.debug("从CSV文件读取流成功.")

    # 将文本解析为UpdateEvent对象
    events_stream = data_stream.map(lambda line: parse_fields(parse_line(line)),
                                    output_type=Types.PICKLED_BYTE_ARRAY()).filter(lambda event: event is not None)
    logger.debug("将操作转化为UpdateEvent实例.")

    # 拆分事件流
    customer_stream = events_stream.filter(lambda event: event.relation == Relation.CUSTOMER)
    orders_stream = events_stream.filter(lambda event: event.relation == Relation.ORDERS)
    lineitem_stream = events_stream.filter(lambda event: event.relation == Relation.LINEITEM)
    logger.debug("事件流拆分成功.")

    # 分层构建处理拓扑
    customer_process = customer_stream.key_by(lambda event: event.payload.c_custkey, key_type=Types.INT()).process(
        CustomerProcess(), output_type=Types.TUPLE([Types.INT(), Types.BOOLEAN()]))
    order_process = orders_stream.key_by(lambda event: event.payload.o_custkey, key_type=Types.INT()).connect(
        customer_process.key_by(lambda x: x[0], key_type=Types.INT())).process(OrdersProcess(), output_type=Types.TUPLE(
        [Types.INT(), Types.BOOLEAN(), Types.PICKLED_BYTE_ARRAY()]))
    lineitem_process = lineitem_stream.key_by(lambda event: event.payload.l_orderkey, key_type=Types.INT()).connect(
        order_process.key_by(lambda x: x[0], key_type=Types.INT())).process(LineitemProcess(),
                                                                            output_type=Types.PICKLED_BYTE_ARRAY())
    arrgregation_process = lineitem_process.key_by(
        lambda rj: (rj.order.o_orderkey, rj.order.o_orderdate, rj.order.o_shippriority),
        key_type=Types.TUPLE([Types.INT(), Types.SQL_DATE(), Types.INT()])).process(AggregationProcess(),
                                                                                    output_type=Types.PICKLED_BYTE_ARRAY())
    logger.debug("处理拓扑构建成功.")

    # 打印最终聚合结果
    arrgregation_process.map(lambda agg: f"OrderKey: {agg.l_orderkey}, OrderDate: {agg.o_orderdate}, "
                                         f"ShipPriority: {agg.o_shippriority}, Revenue: {agg.revenue}",
                             output_type=Types.STRING()).print()

    # 执行Flink作业
    flink_env.execute("激活流处理作业")
