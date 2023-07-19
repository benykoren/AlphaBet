import logging
import pickle
import sqlite3
from typing import List

from dataclesses import Account, Status, Transaction
from exceptions import AccountNotExist


class AccountData:
    def __init__(self, connection):
        self._con = connection
        self._cur = self._con.cursor()
        self.logger = logging.getLogger("AccountData")

    def create_table(self):
        self._cur.execute("CREATE TABLE IF NOT EXISTS accounts("
                          "account_id INTEGER PRIMARY KEY,"
                          "balance REAL,"
                          "loan BLOB)")
        self._con.commit()

    def add_account(self, account: Account) -> Status:
        try:
            loan_data = pickle.dumps(account.loan) if account.loan else None
            sql = '''INSERT INTO accounts(account_id, balance, loan) VALUES(?, ?, ?)'''
            self._cur.execute(sql, (account.account_id, account.balance, loan_data))
            self._con.commit()
            self.logger.info(f"Account added: Account ID: {account.account_id}")
            return Status.SUCCESS
        except sqlite3.Error as e:
            self.logger.error("An error occurred while adding an account:", exc_info=True)
            raise e

    def update_account(self, account: Account) -> Status:
        try:
            loan_data = pickle.dumps(account.loan) if account.loan else None
            sql = "UPDATE accounts SET balance=?, loan=? WHERE account_id=?"
            self._cur.execute(sql, (account.balance, loan_data, account.account_id))
            self._con.commit()
            self.logger.info(f"Account updated: Account ID: {account.account_id}")
            return Status.SUCCESS
        except sqlite3.Error as e:
            self.logger.error("An error occurred while updating an account:", exc_info=True)
            raise e

    def delete_account(self, account_id: int) -> Status:
        try:
            sql = "DELETE FROM accounts WHERE account_id=?"
            self._cur.execute(sql, (account_id,))
            self._con.commit()
            self.logger.info(f"Account deleted: Account ID: {account_id}")
            return Status.SUCCESS
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting an account:", exc_info=True)
            raise e

    def delete_all_accounts(self) -> Status:
        try:
            sql = "DELETE FROM accounts"
            self._cur.execute(sql)
            self._con.commit()
            self.logger.info("All accounts deleted")
            return Status.SUCCESS
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting all accounts:", exc_info=True)
            raise e

    def get_account(self, account_id: int) -> Account:
        try:
            sql = "SELECT * FROM accounts WHERE account_id=?"
            result = self._cur.execute(sql, (account_id,)).fetchone()
            if result:
                account_id, balance, loan_data = result
                loan = pickle.loads(loan_data) if loan_data else None
                account = Account(account_id, balance, loan)
                self.logger.info(f"Account retrieved: Account ID: {account_id}")
                return account
            else:
                raise AccountNotExist
        except sqlite3.Error as e:
            self.logger.error("An error occurred while getting an account:", exc_info=True)
            raise e

    def get_all_accounts(self) -> List[Account]:
        try:
            sql = "SELECT * FROM accounts"
            results = self._cur.execute(sql).fetchall()
            accounts = []
            for result in results:
                account_id, balance, loan_data = result
                loan = pickle.loads(loan_data) if loan_data else None
                account = Account(account_id, balance, loan)
                accounts.append(account)
            self.logger.info("All accounts retrieved")
            return accounts
        except sqlite3.Error as e:
            self.logger.error("An error occurred while getting all accounts:", exc_info=True)
            raise e


class TransactionData:
    def __init__(self, connection):
        self._con = connection
        self._cur = self._con.cursor()
        self.logger = logging.getLogger("TransactionData")

    def create_table(self):
        self._cur.execute("CREATE TABLE IF NOT EXISTS transactions("
                          "transaction_id INTEGER PRIMARY KEY, "
                          "amount REAL, "
                          "date TEXT, "
                          "src_bank_account INTEGER, "
                          "dst_bank_account INTEGER, "
                          "direction TEXT, "
                          "status TEXT)")
        self._con.commit()

    def add_transaction(self, transaction: Transaction) -> Status:
        try:
            sql = "INSERT INTO transactions(transaction_id, amount, date, src_bank_account," \
                  " dst_bank_account, direction, status) VALUES (?, ?, ?, ?, ?, ?, ?)"
            self._cur.execute(
                sql,
                (
                    transaction.transaction_id,
                    transaction.amount,
                    transaction.date,
                    transaction.src_bank_account,
                    transaction.dst_bank_account,
                    transaction.direction,
                    transaction.status,
                ),
            )
            self._con.commit()
            self.logger.info(f"Transaction added: Transaction ID: {transaction.transaction_id}")
            return Status.SUCCESS
        except sqlite3.Error as e:
            self.logger.error("An error occurred while adding a transaction:", exc_info=True)
            raise e

    def delete_transaction(self, transaction_id: int) -> Status:
        try:
            sql = "DELETE FROM transactions WHERE transaction_id=?"
            self._cur.execute(sql, (transaction_id,))
            self._con.commit()
            self.logger.info(f"Transaction deleted: Transaction ID: {transaction_id}")
            return Status.SUCCESS
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting a transaction:", exc_info=True)
            raise e

    def delete_all_transactions(self) -> Status:
        try:
            sql = "DELETE FROM transactions"
            self._cur.execute(sql)
            self._con.commit()
            self.logger.info("All transactions deleted")
            return Status.SUCCESS
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting all transactions:", exc_info=True)
            raise e

    def get_all_transactions(self) -> List[Transaction]:
        try:
            sql = "SELECT * FROM transactions"
            results = self._cur.execute(sql).fetchall()
            self.logger.info("All transactions retrieved")
            return results
        except sqlite3.Error as e:
            self.logger.error("An error occurred while getting all transactions:", exc_info=True)
            raise e


class DataLayer:
    def __init__(self, connection_string):
        self._con = sqlite3.connect(connection_string)
        self.logger = logging.getLogger("DataLayer")

        self.account_data = AccountData(self._con)
        self.transaction_data = TransactionData(self._con)

    def create_tables(self):
        self.account_data.create_table()
        self.transaction_data.create_table()

    def delete_all_data(self):
        self.account_data.delete_all_accounts()
        self.transaction_data.delete_all_transactions()
