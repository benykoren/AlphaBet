from typing import List


class Processor:
    @staticmethod
    def perform_transaction(src_bank_account: int, dst_bank_account: int, amount: float, direction: str) -> int:
        return 2

    @staticmethod
    def download_report() -> List:
        return [
            (1, "success"),
            (2, "fail"),
            (3, "success"),
            (4, "fail"),
        ]
