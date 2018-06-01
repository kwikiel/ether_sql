from sqlalchemy import (
    Column,
    String,
    Numeric,
    ForeignKey,
    Text,
    Integer,
    TIMESTAMP
)
from sqlalchemy.orm import relationship
import logging
from web3.utils.formatters import hex_to_integer
from ether_sql.models import base
from ether_sql.models.storageDiff import StorageDiff

logger = logging.getLogger(__name__)


class StateDiff(base):
    """
    Class mapping a state_diff table in psql to a difference in state after transactions
    :param int block_number: Number of the block containing this StateDiff
    :param timestamp timestamp: Unix time at the at this block
    :param str transaction_hash: The transaction hash if this was created by a transaction
    :param int transaction_index: Position of this transaction in the transaction list of the block
    :param str address: Account address where the change occoured
    :param int balance_from: Initial balance of this account
    :param int balance_to: Final balance of this account
    :param str code_from: Initial code of this account
    :param str code_to: Final code of this account
    """

    __tablename__ = 'state_diff'
    id = Column(Integer, primary_key=True, autoincrement=True)
    block_number = Column(Numeric, ForeignKey('blocks.block_number'))
    timestamp = Column(TIMESTAMP)
    # nullable because some state changes also occour because of miner rewards
    transaction_hash = Column(String(66),
                              ForeignKey('transactions.transaction_hash'),
                              nullable=True,
                              index=True)
    transaction_index = Column(Numeric, nullable=True)
    address = Column(String(42), index=True)
    balance_from = Column(Numeric, nullable=True)
    balance_to = Column(Numeric, nullable=True)
    nonce_from = Column(Integer, nullable=True)
    nonce_to = Column(Integer, nullable=True)
    code_from = Column(Text, nullable=True)
    code_to = Column(Text, nullable=True)
    storage_diff = relationship('StorageDiff', backref='state_diff')

    def to_dict(self):
        return {
            'block_number': self.block_number,
            'timestamp': self.timestamp,
            'transaction_hash': self.transaction_hash,
            'transaction_index': self.transaction_index,
            'address': self.address,
            'balance_from': self.balance_from,
            'balance_to': self.balance_to,
            'code_from': self.code_from,
            'code_to': self.code_to,
        }

    def _parseStateDiff(account_state, type):
        state_from = None
        state_to = None

        if isinstance(account_state, dict):
            key = list(account_state)
            if key[0] == '*':
                if type == 'code':
                    state_from = account_state['*']['from']
                    state_to = account_state['*']['to']
                else:
                    state_from = hex_to_integer(account_state['*']['from'])
                    state_to = hex_to_integer(account_state['*']['to'])
            elif key[0] == '+':
                if type == 'code':
                    state_to = account_state['+']
                else:
                    state_to = hex_to_integer(account_state['+'])
            else:
                logger.error('Unknown key in account state')
        elif account_state == '=':
            logger.debug('No change in type'.format(type))
        return state_from, state_to

    @classmethod
    def add_state_diff(cls, state_diff_row, address, transaction_hash,
                       transaction_index, block_number, timestamp):
        balance_from, balance_to = cls._parseStateDiff(
                                    state_diff_row['balance'], 'balance')
        nonce_from, nonce_to = cls._parseStateDiff(
                                    state_diff_row['nonce'], 'nonce')
        code_from, code_to = cls._parseStateDiff(
                                    state_diff_row['code'], 'code')

        state_diff = cls(block_number=block_number,
                         timestamp=timestamp,
                         transaction_hash=transaction_hash,
                         transaction_index=transaction_index,
                         address=address,
                         balance_from=balance_from,
                         balance_to=balance_to,
                         nonce_from=nonce_from,
                         nonce_to=nonce_to,
                         code_from=code_from,
                         code_to=code_to)
        return state_diff

    @classmethod
    def add_state_diff_dict(cls, session, state_diff_dict, transaction_hash,
                            transaction_index, block_number, timestamp):
        for address in state_diff_dict:
            state_diff = cls.add_state_diff(state_diff_row=state_diff_dict[address],
                                            address=address,
                                            transaction_hash=transaction_hash,
                                            transaction_index=transaction_index,
                                            block_number=block_number,
                                            timestamp=timestamp)
            session.db_session.add(state_diff)
            session.flush()

            if state_diff_dict[address]['storage'] is not {}:
                session = StorageDiff.\
                    add_storage_diff_dict(session=session,
                                          storage_diff_dict=state_diff_dict[address]['storage'],
                                          state_diff_id=state_diff.id,
                                          address=address,
                                          transaction_hash=transaction_hash,
                                          transaction_index=transaction_index,
                                          block_number=block_number,
                                          timestamp=timestamp)
        return session
