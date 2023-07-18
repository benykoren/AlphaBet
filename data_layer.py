import logging
import pickle
import sqlite3

from dataclesses import Account
from exceptions import AccountNotExist


class DataLayer:
    def __init__(self, connection_string):
        self.con = sqlite3.connect(connection_string)
        self.cur = self.con.cursor()
        self.create_tables()
        self.logger = logging.getLogger("DataLayer")

    def create_tables(self):
        self.cur.execute("CREATE TABLE IF NOT EXISTS accounts("
                         "account_id INTEGER PRIMARY KEY,"
                         "balance REAL,"
                         "loan BLOB)")
        self.cur.execute("CREATE TABLE IF NOT EXISTS transactions("
                         "transaction_id INTEGER PRIMARY KEY, "
                         "amount REAL, "
                         "date TEXT, "
                         "src_bank_account INTEGER, "
                         "dst_bank_account INTEGER, "
                         "direction TEXT, "
                         "status TEXT)")

    def add_account(self, account):
        try:
            loan_data = pickle.dumps(account.loan) if account.loan else None
            sql = f''' INSERT INTO accounts(account_id, balance, loan) VALUES(?, ?, ?) '''
            self.cur.execute(sql, (account.account_id, account.balance, loan_data))
            self.con.commit()
            self.logger.info(f"Account added: Account ID: {account.account_id}")
        except sqlite3.Error as e:
            self.logger.error("An error occurred while adding an account:", exc_info=True)

    def update_account(self, account):
        try:
            loan_data = pickle.dumps(account.loan) if account.loan else None
            sql = "UPDATE accounts SET balance=?, loan=? WHERE account_id=?"
            self.cur.execute(sql, (account.balance, loan_data, account.account_id))
            self.con.commit()
            self.logger.info(f"Account updated: Account ID: {account.account_id}")
        except sqlite3.Error as e:
            self.logger.error("An error occurred while updating an account:", exc_info=True)

    def delete_account(self, account_id):
        try:
            sql = "DELETE FROM accounts WHERE account_id=?"
            self.cur.execute(sql, (account_id,))
            self.con.commit()
            self.logger.info(f"Account deleted: Account ID: {account_id}")
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting an account:", exc_info=True)

    def delete_all_accounts(self):
        try:
            sql = "DELETE FROM accounts"
            self.cur.execute(sql)
            self.con.commit()
            self.logger.info("All accounts deleted")
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting all accounts:", exc_info=True)

    def get_account(self, account_id):
        try:
            sql = "SELECT * FROM accounts WHERE account_id=?"
            result = self.cur.execute(sql, (account_id,)).fetchone()
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

    def get_all_accounts(self):
        try:
            sql = "SELECT * FROM accounts"
            results = self.cur.execute(sql).fetchall()
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

    def add_transaction(self, transaction):
        try:
            sql = "INSERT INTO transactions(transaction_id, amount, date, src_bank_account," \
                  " dst_bank_account, direction, status) VALUES (?, ?, ?, ?, ?, ?, ?)"
            self.cur.execute(
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
            self.con.commit()
            self.logger.info(f"Transaction added: Transaction ID: {transaction.transaction_id}")
        except sqlite3.Error as e:
            self.logger.error("An error occurred while adding a transaction:", exc_info=True)

    def delete_transaction(self, transaction_id):
        try:
            sql = "DELETE FROM transactions WHERE transaction_id=?"
            self.cur.execute(sql, (transaction_id,))
            self.con.commit()
            self.logger.info(f"Transaction deleted: Transaction ID: {transaction_id}")
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting a transaction:", exc_info=True)

    def delete_all_transaction(self):
        try:
            sql = "DELETE FROM transactions"
            self.cur.execute(sql)
            self.con.commit()
            self.logger.info("All transactions deleted")
        except sqlite3.Error as e:
            self.logger.error("An error occurred while deleting all transactions:", exc_info=True)

    def get_all_transactions(self):
        try:
            sql = "SELECT * FROM transactions"
            results = self.cur.execute(sql).fetchall()
            self.logger.info("All transactions retrieved")
            return results
        except sqlite3.Error as e:
            self.logger.error("An error occurred while getting all transactions:", exc_info=True)
