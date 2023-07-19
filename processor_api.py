from typing import Tuple, List

from dataclesses import Transaction, Status
from exceptions import TransactionError
from processor import Processor


class ProcessorApi:
    def __init__(self):
        self.processor = Processor()

    def perform_transaction(self, transaction: Transaction) -> Tuple[Status, int]:
        transaction_id = self.processor.perform_transaction(src_bank_account=transaction.src_bank_account,
                                                            dst_bank_account=transaction.dst_bank_account,
                                                            amount=transaction.amount,
                                                            direction=transaction.direction)
        if transaction_id is not None:
            report = self.download_report()
            if report[transaction_id] == Status.SUCCESS.value:
                return Status.SUCCESS.value, transaction_id
            else:
                return Status.FAIL.value, transaction_id
        raise TransactionError

    def download_report(self) -> List[Tuple[int, str]]:
        return self.processor.download_report()
