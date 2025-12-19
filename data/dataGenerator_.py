import os

def reset_line_format(line: str) -> str:
    """
    重置数据行的格式，在行首添加"|"，在行尾删除"|"
    """
    if not line:
        return ""
    line = line.strip()
    if not line.startswith("|"):
        line = "|" + line
    if line.endswith("|"):
        line = line[:-1]
    return line + "\n"

def q3_data_generator():
    """
    生成 TPC-H Q3 查询的插入/删除数据流，包含 lineitem、orders 和 customer 表的数据。
    """
    data_file_path_prefix = "./1GB/"   # TPC-H 数据文件路径
    output_file_path = os.path.join(data_file_path_prefix, "q3_output_1GB.csv")  # 输出文件路径
    window_size = 100000  # 窗口大小，控制删除操作的延迟

    # 文件路径
    customer_file = os.path.join(data_file_path_prefix, "customer.tbl")
    orders_file = os.path.join(data_file_path_prefix, "orders.tbl")
    lineitem_file = os.path.join(data_file_path_prefix, "lineitem.tbl")

    for f in [customer_file, orders_file, lineitem_file]:
        if not os.path.exists(f):
            raise FileNotFoundError(f"{f} not found.")

    output = open(output_file_path, "w")
    lineitem = open(lineitem_file, "r")
    lineitem_d = open(lineitem_file, "r")
    orders = open(orders_file, "r")
    orders_d = open(orders_file, "r")
    customer = open(customer_file, "r")
    customer_d = open(customer_file, "r")

    # 初始化计数
    count = 0
    delete_count = 0 - window_size
    orders_count = 0
    orders_delete_count = 0
    customer_count = 0
    customer_delete_count = 0

    # 初始化数据行
    line_lineitem = lineitem.readline()
    line_lineitem_d = lineitem_d.readline()
    line_orders = orders.readline()
    line_orders_d = orders_d.readline()
    line_customer = customer.readline()
    line_customer_d = customer_d.readline()
    
    # 在头部添加"|"，尾部删去"|"
    line_lineitem = reset_line_format(line_lineitem)
    line_lineitem_d = reset_line_format(line_lineitem_d)
    line_orders = reset_line_format(line_orders)
    line_orders_d = reset_line_format(line_orders_d)
    line_customer = reset_line_format(line_customer)
    line_customer_d = reset_line_format(line_customer_d)

    # 生成插入/删除流
    while line_lineitem:
        count += 1
        delete_count += 1

        # 插入 lineitem
        output.write("+LI" + line_lineitem)
        line_lineitem = lineitem.readline()
        line_lineitem = reset_line_format(line_lineitem)

        # 插入 orders
        if count % 4 == 0 and line_orders: # 控制 orders 插入频率
            orders_count += 1
            output.write("+OR" + line_orders)
            line_orders = orders.readline()
            line_orders = reset_line_format(line_orders)

        # 插入 customer
        if count % 40 == 0 and line_customer:
            customer_count += 1
            output.write("+CU" + line_customer)
            line_customer = customer.readline()
            line_customer = reset_line_format(line_customer)

        # 删除 lineitem
        if delete_count > 0:
            output.write("-LI" + line_lineitem_d)
            line_lineitem_d = lineitem_d.readline()
            line_lineitem_d = reset_line_format(line_lineitem_d)

            # 删除 orders
            if delete_count % 4 == 0 and line_orders_d:
                orders_delete_count += 1
                output.write("-OR" + line_orders_d)
                line_orders_d = orders_d.readline()
                line_orders_d = reset_line_format(line_orders_d)

            # 删除 customer
            if delete_count % 40 == 0 and line_customer_d:
                customer_delete_count += 1
                output.write("-CU" + line_customer_d)
                line_customer_d = customer_d.readline()
                line_customer_d = reset_line_format(line_customer_d)

    # 关闭
    lineitem.close()
    lineitem_d.close()
    orders.close()
    orders_d.close()
    customer.close()
    customer_d.close()
    output.close()

    print("Q3 data generation finished.")


if __name__ == "__main__":
    q3_data_generator()
