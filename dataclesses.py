from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import List


class Direction(Enum):
    CREDIT = "credit"
    DEBIT = "debit"


class Status(Enum):
    SUCCESS = "success"
    FAIL = "fail"


MANAGER_ACCOUNT = 0


@dataclass
class Transaction:
    amount: float
    direction: Direction
    date: date
    src_bank_account: int = MANAGER_ACCOUNT
    dst_bank_account: int = MANAGER_ACCOUNT
    status: Status = None
    transaction_id: int = None


@dataclass
class Loan:
    debt: float
    transactions: List[Transaction]


@dataclass
class Account:
    account_id: int
    balance: float
    loan: Loan = None
