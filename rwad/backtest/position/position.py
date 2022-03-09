from typing import List, Dict
import time


class Bill:
    def __init__(self, code: str, transaction_price: float, share: int, timestamp: str = None):
        self.code = code
        self.transaction_price = transaction_price
        self.share = share
        self.timestamp = timestamp

    def exact(self):
        return self.code, self.price, self.share, self.timestamp


class Holding:
    def __init__(self, code: str, share: int, cost_price: float, price: float, profit: float):
        self.code = code
        self.share = share
        self.cost_price = cost_price
        self.price = price
        self.profit = profit

    def update(self, share, cost_price, price, profit):
        self.share = share
        self.cost_price = cost_price
        self.price = price
        self.profit = profit


class Position:
    def __init__(self, init_balance: float = 1000000.0, slippage: float = 0.03, charge: float = 0.0005):
        """
        This class is used to simulate actual account positions.
        :param init_balance: initial balance, defalut: 10 million
        :param slippage: transaction error, greater than 0.0
        :param charge: service charges need to be paid by securities firms when trading, greater than 0.0
        """
        assert slippage >= 0. and charge >= 0., "Fees cannot be negative."

        self.balance = init_balance
        self.slippage = slippage
        self.charge = charge
        self.total_assets = self.balance
        self.total_profit = 0.
        self.holdings = {}
        self.bills = []
        # TODO: portfolio combination risk control may be implemented in the future.
        # self.sharpe_ratio = 0.0

    def buy(self, operations: List[Bill], time_rec: bool = True):
        for bill in operations:
            code, price, share, timestamp = bill.exact()
            fixed_price = price * (1. + self.slippage) * (1. + self.charge)
            amount = share * 100 * fixed_price
            if self.balance < amount:
                return
            self.balance -= amount
            if code in self.holdings.keys():
                stock = self.holdings[code]
                new_cost_price = (stock.cost_price * stock.share + fixed_price * share) / (stock.share + share)
                new_share = stock.share + share
                self.holdings[code].update(new_share, new_cost_price, price,
                                           self.holdings[code].profit - (fixed_price - price) * share)
            else:
                self.holdings[code] = Holding(code, share, fixed_price, price, -(fixed_price - price) * share)
            if time_rec is True:
                self.bills.append(Bill(code, fixed_price, share, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            else:
                self.bills.append(Bill(code, fixed_price, share))
            self.total_assets -= (fixed_price - price) * share
            self.update()


    def sell(self, operations: List[Bill]):
        pass

    def update(self):
        pass