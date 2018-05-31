from sqlalchemy import (
    Column,
    String,
    Numeric,
    ForeignKey,
    Text,
    Integer,
    TIMESTAMP
)
import logging
from web3.utils.encoding import to_hex
from web3.utils.formatters import hex_to_integer
from ether_sql.models import base

logger = logging.getLogger(__name__)


class StateDiff(base):
    """
    Class mapping a stateDiff table in psql to a difference in state after transactions
    """

    __tablename__ = 'stateDiff'
    id = Column(Integer, primary_key=True)
    block_number = Column(Numeric, ForeignKey('blocks.block_number'))
    timestamp = Column(TIMESTAMP)
    transaction_hash = Column(String(66),
                              ForeignKey('transactions.transaction_hash'),
                              index=True)
    transaction_index = Column(Numeric, nullable=True)
    stateDiff_index = Column(Integer, nullable=False)
    address = Column(String(42), index=True)
    balance_from = Column(Numeric, nullable=True)
    balance_to = Column(Numeric, nullable=True)
    nonce_from = Column(Integer, nullable=True)
    nonce_to = Column(Integer, nullable=True)
    code_from = Column(Text, nullable=True)
    code_to = Column(Text, nullable=True)

    def to_dict(self):
        return {
            'block_number': self.block_number,
            'timestamp': self.timestamp,
            'transaction_hash': self.transaction_hash,
            'transaction_index': self.transaction_index,
            'stateDiff_index': self.stateDiff_index,
            'address': self.address,
            'balance_from': self.balance_from,
            'balance_to': self.balance_to,
            'code_from': self.code_from,
            'code_to': self.code_to,
        }
