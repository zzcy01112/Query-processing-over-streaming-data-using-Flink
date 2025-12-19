class Customer:
    """
    Customer类，只保留Q3中使用的字段
    """
    def __init__(self, c_custkey: int, c_mktsegment: str):
        self.c_custkey = c_custkey
        self.c_mktsegment = c_mktsegment
