import os
import unittest
import csv

from portfolio import build_portfolio


class PortfolioRulesTest(unittest.TestCase):
    def setUp(self):
        self.sp = os.path.join("data", "sp500.csv")
        self.nd = os.path.join("data", "nasdaq100.csv")
        self.ru = os.path.join("data", "russell3000.csv")
        self.out = "tests_output_portfolio.csv"
        if os.path.exists(self.out):
            os.remove(self.out)

    def tearDown(self):
        if os.path.exists(self.out):
            os.remove(self.out)

    def test_constraints(self):
        build_portfolio(self.sp, self.nd, self.ru, self.out)
        tickers = []
        weights = []
        with open(self.out, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                tickers.append(row["ticker"])
                weights.append(float(row["weight_pct"]))

        # no duplicates
        self.assertEqual(len(tickers), len(set(tickers)))
        # total holdings <= 100
        self.assertLessEqual(len(tickers), 100)
        # no position > 8%
        for w in weights:
            self.assertLessEqual(w, 8.0001)
        # weights sum approx 100
        self.assertAlmostEqual(sum(weights), 100.0, places=3)


if __name__ == "__main__":
    unittest.main()
