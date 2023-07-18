import logging
import time
from datetime import timedelta, date

import schedule as schedule

from data_layer import DataLayer, AccountNotExist
from dataclesses import Transaction, Status, Account, Loan, Direction
from exceptions import TransactionError
from processor_api import ProcessorApi

NUMBER_OF_PAYMENTS = 12
PAYMENTS_DELTA = 7

logging.basicConfig(filename='bank_service.log', level=logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
root_logger = logging.getLogger()
root_logger.addHandler(console_handler)


class BankService:
    def __init__(self):
        self.processor_api = ProcessorApi()
        self.data_layer = DataLayer("bank.db")

    def add_account(self, account: Account):
        self.data_layer.add_account(account)
        logging.info(f"Account added: {account.account_id}")

    def perform_advance(self, dst_bank_account: int, amount):
        transaction = Transaction(dst_bank_account=dst_bank_account, amount=amount, direction=Direction.CREDIT.value,
                                  date=date.today())
        try:
            account: Account = self.data_layer.get_account(dst_bank_account)
            self.perform_transaction(transaction=transaction)
            self.add_loan(account=account, transaction=transaction)
            logging.info(f"Advance performed: Transaction ID: {transaction.transaction_id}")
            return transaction.transaction_id
        except AccountNotExist:
            transaction.status = Status.FAIL
            self.add_transaction_to_data_layer(transaction)
            logging.error("Account does not exist.")
            raise
        except TransactionError:
            logging.error("Transaction failed.")
            raise

    def perform_transaction(self, transaction: Transaction):
        try:
            transaction.status, transaction.transaction_id = self.processor_api.perform_transaction(transaction)
            logging.info(f"Transaction performed: Transaction ID: {transaction.transaction_id}")
        except TransactionError:
            transaction.status = Status.FAIL
            logging.error("Transaction failed.")
            raise
        finally:
            self.add_transaction_to_data_layer(transaction)

    def add_transaction_to_data_layer(self, transaction):
        self.data_layer.add_transaction(transaction)
        logging.info(f"Transaction added to DataBase: Transaction ID: {transaction.transaction_id}")

    def add_loan(self, account: Account, transaction: Transaction):
        loan_transactions = []
        loan_debt = transaction.amount

        for index, payment in enumerate(range(NUMBER_OF_PAYMENTS), start=1):
            payment_amount = transaction.amount / NUMBER_OF_PAYMENTS
            payment_date = date.today() + timedelta(days=PAYMENTS_DELTA * index)
            loan_payment = Transaction(amount=payment_amount,
                                       date=payment_date,
                                       src_bank_account=transaction.dst_bank_account,
                                       direction=Direction.DEBIT)
            loan_transactions.append(loan_payment)

        account.loan = Loan(debt=loan_debt, transactions=loan_transactions)
        account.balance += transaction.amount
        self.data_layer.update_account(account=account)
        logging.info(f"Loan added for Account: {account.account_id}")

    def loan_payments(self):
        accounts = self.data_layer.get_all_accounts()

        for account in accounts:
            if account.loan:
                weekly_transactions = [
                    transaction for transaction in account.loan.transactions if transaction.date == date.today()]
                for transaction in weekly_transactions:
                    try:
                        self.perform_transaction(transaction=transaction)
                        account.balance -= transaction.amount
                        self.data_layer.update_account(transaction)
                        logging.info(f"Loan payment performed for Account: {account.account_id}")
                    except TransactionError:
                        last_payment = -1
                        transaction.date = account.loan.transactions[last_payment] + timedelta(days=PAYMENTS_DELTA)
                        account.loan.transactions.append(transaction)
                        logging.error(f"Loan payment failed for Account: {account.account_id}")
                        raise

    def download_report(self):
        report = self.processor_api.download_report()
        logging.info("Report downloaded")
        return report


def perform_payments():
    b = BankService()
    b.loan_payments()
    logging.info("payments performed.")


if __name__ == '__main__':
    schedule.every().day.at("09:00").do(perform_payments)

    # Keep the program running and continuously check the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)
