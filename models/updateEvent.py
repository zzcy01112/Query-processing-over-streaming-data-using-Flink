from typing import Union

from models.customer import Customer
from models.lineitem import Lineitem
from models.orders import Orders


class UpdateEvent:
    """
    定义更新操作事件
    """
    def __init__(self, operator: str, relation: str, payload: Union[Customer, Orders, Lineitem]):
        self.operator = operator
        self.relation = relation
        self.payload = payload
