import time

from pyflink.table import EnvironmentSettings, TableEnvironment, Schema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common import Types, Row
from modules.logger_setup import logger


def run_table_api():
    """
    运行 Table API 模式的查询
    :return: None
    """
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # 注册表
    t_env.execute_sql("""
                      CREATE
                      TEMPORARY TABLE customer (
                c_custkey INT,
                c_name STRING,
                c_address STRING,
                c_nationkey INT,
                c_phone STRING,
                c_acctbal DOUBLE,
                c_mktsegment STRING,
                c_comment STRING
            ) WITH (
                'connector' = 'filesystem',
                'path' = 'file:///mnt/f/Codefield/Code_Python/FlinkQueryStreamProcessing/data/1GB/customer.tbl',
                'format' = 'csv',
                'csv.field-delimiter' = '|',
                'csv.ignore-parse-errors' = 'true'
            )
                      """)

    t_env.execute_sql("""
                      CREATE
                      TEMPORARY TABLE orders (
            o_orderkey INT,
            o_custkey INT,
            o_orderstatus STRING,
            o_totalprice DOUBLE,
            o_orderdate DATE,
            o_orderpriority STRING,
            o_clerk STRING,
            o_shippriority INT,
            o_comment STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///mnt/f/Codefield/Code_Python/FlinkQueryStreamProcessing/data/1GB/orders.tbl',
            'format' = 'csv',
            'csv.field-delimiter' = '|',
            'csv.ignore-parse-errors' = 'true'
        )
                      """)

    t_env.execute_sql("""
                      CREATE
                      TEMPORARY TABLE lineitem (
            l_orderkey INT,
            l_partkey INT,
            l_suppkey INT,
            l_linenumber INT,
            l_quantity DOUBLE,
            l_extendedprice DOUBLE,
            l_discount DOUBLE,
            l_tax DOUBLE,
            l_returnflag STRING,
            l_linestatus STRING,
            l_shipdate DATE,
            l_commitdate DATE,
            l_receiptdate DATE,
            l_shipinstruct STRING,
            l_shipmode STRING,
            l_comment STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///mnt/f/Codefield/Code_Python/FlinkQueryStreamProcessing/data/1GB/lineitem.tbl',
            'format' = 'csv',
            'csv.field-delimiter' = '|',
            'csv.ignore-parse-errors' = 'true'
        )
                      """)

    start = time.time()
    result = t_env.execute_sql("""
                               SELECT l_orderkey,
                                      SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                                      o_orderdate,
                                      o_shippriority
                               FROM customer,
                                    orders,
                                    lineitem
                               WHERE c_mktsegment = 'BUILDING'
                                 AND c_custkey = o_custkey
                                 AND l_orderkey = o_orderkey
                                 AND o_orderdate < DATE '1995-03-15'
                                 AND l_shipdate > DATE '1995-03-15'
                               GROUP BY l_orderkey, o_orderdate, o_shippriority
                               ORDER BY revenue DESC, o_orderdate LIMIT 10
                               """)
    result.print()
    logger.info(f"Flink Table API 耗时: {time.time() - start} 秒")


def run_stream_api():
    """
    运行 DataStream API 模式的直接查询
    :return: None
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    ds_customer = env.read_text_file(
        "file:///mnt/f/Codefield/Code_Python/FlinkQueryStreamProcessing/data/1GB/customer.tbl") \
        .map(lambda line: line.strip().split("|"),
             output_type=Types.ROW_NAMED(
                 ["c_custkey", "c_mktsegment"],
                 [Types.INT(), Types.STRING()]
             )) \
        .map(lambda fields: Row(int(fields[0]), fields[6]),
             output_type=Types.ROW_NAMED(
                 ["c_custkey", "c_mktsegment"],
                 [Types.INT(), Types.STRING()]
             ))

    ds_orders = env.read_text_file(
        "file:///mnt/f/Codefield/Code_Python/FlinkQueryStreamProcessing/data/1GB/orders.tbl") \
        .map(lambda line: line.strip().split("|"),
             output_type=Types.ROW_NAMED(
                 ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"],
                 [Types.INT(), Types.INT(), Types.STRING(), Types.INT()]
             )) \
        .map(lambda fields: Row(int(fields[0]), int(fields[1]), fields[4], int(fields[7])),
             output_type=Types.ROW_NAMED(
                 ["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"],
                 [Types.INT(), Types.INT(), Types.STRING(), Types.INT()]
             ))

    ds_lineitem = env.read_text_file(
        "file:///mnt/f/Codefield/Code_Python/FlinkQueryStreamProcessing/data/1GB/lineitem.tbl") \
        .map(lambda line: line.strip().split("|"),
             output_type=Types.ROW_NAMED(
                 ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
                 [Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.STRING()]
             )) \
        .map(lambda fields: Row(int(fields[0]), float(fields[5]), float(fields[6]), fields[10]),
             output_type=Types.ROW_NAMED(
                 ["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
                 [Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.STRING()]
             ))

    # 注册表
    t_env.create_temporary_view("customer", ds_customer)
    t_env.create_temporary_view("orders", ds_orders)
    t_env.create_temporary_view("lineitem", ds_lineitem)

    start = time.time()
    result = t_env.execute_sql("""
                               SELECT l_orderkey,
                                      SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                                      o_orderdate,
                                      o_shippriority
                               FROM customer,
                                    orders,
                                    lineitem
                               WHERE c_mktsegment = 'BUILDING'
                                 AND c_custkey = o_custkey
                                 AND l_orderkey = o_orderkey
                                 AND o_orderdate < DATE '1995-03-15'
                                 AND l_shipdate > DATE '1995-03-15'
                               GROUP BY l_orderkey, o_orderdate, o_shippriority
                               ORDER BY revenue DESC, o_orderdate LIMIT 10
                               """)
    result.print()
    logger.info(f"Flink DataStream API 耗时: {time.time() - start} 秒")


if __name__ == "__main__":
    run_table_api()
    run_stream_api()
