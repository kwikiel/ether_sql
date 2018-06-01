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
from web3.utils.formatters import hex_to_integer
from ether_sql.models import base

logger = logging.getLogger(__name__)


class StateDiff(base):
    """
    Class mapping a state_diff table in psql to a difference in state after transactions
    """

    __tablename__ = 'state_diff'
    id = Column(Integer, primary_key=True)
    block_number = Column(Numeric, ForeignKey('blocks.block_number'))
    timestamp = Column(TIMESTAMP)
    transaction_hash = Column(String(66),
                              ForeignKey('transactions.transaction_hash'),
                              index=True)
    transaction_index = Column(Numeric, nullable=True)
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
            'address': self.address,
            'balance_from': self.balance_from,
            'balance_to': self.balance_to,
            'code_from': self.code_from,
            'code_to': self.code_to,
        }

    def _parseStateDiff(account_state, type):
        state_from = ''
        state_to = ''

        keys = account_state.keys()
        if keys[0] == '*':
            logging.debug('Key is *')
            if type == 'code':
                state_from = account_state['*']['from']
                state_to = account_state['*']['to']
            else:
                state_from = hex_to_integer(account_state['*']['from'])
                state_to = hex_to_integer(account_state['*']['to'])
        elif keys[0] == '+':
            logging.debug('Key is +')
            state_from = ''
            if type == 'code':
                state_to = account_state['+']
            else:
                state_to = hex_to_integer(account_state['+'])
        else:
            logging.error('Unknown key in account state')

        return state_from, state_to

    @classmethod
    def add_state_diff(cls, state_diff, address, transaction_hash,
                       transaction_index, block_number, timestamp):
        balance_from, balance_to = cls._parseStateDiff(
                                    state_diff['balance'], 'balance')
        nonce_from, nonce_to = cls._parseStateDiff(
                                    state_diff['nonce'], 'nonce')
        code_from, code_to = cls._parseStateDiff(
                                    state_diff['code'], 'code')

        if state_diff['storage'] is not {}:
            logger.debug('storage at {}: {}'.format(block_number, state_diff['storage']))

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
            state_diff = cls.add_state_diff(state_diff=state_diff_dict[address],
                                            address=address,
                                            transaction_hash=transaction_hash,
                                            transaction_index=transaction_index,
                                            block_number=block_number,
                                            timestamp=timestamp)
            session.db_session.add(state_diff)
        return session
