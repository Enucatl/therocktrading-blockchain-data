import abc

import click
import pandas as pd
import requests


class TransactionFinder(abc.ABC):
    date_format = "%Y-%m-%d"

    def __init__(
        self, currency, endpoint, factor, key, base_url="https://api.blockchair.com"
    ):
        chain = currency
        self._factor = factor
        self._url = f"{base_url}/{chain}/{endpoint}"
        self._key = key

    def find_transactions(self, d):
        date_from = (d["Date"] - pd.Timedelta(days=1)).strftime(self.date_format)
        date_to = (d["Date"] + pd.Timedelta(days=1)).strftime(self.date_format)
        params = {
            "q": (
                f"time({date_from}..{date_to}),"
                f"value({abs(int(d['Price'] * self._factor))})"
            ),
            "key": self._key,
        }
        if d["Note"]:
            params["q"] = (
                f"time({date_from}..{date_to}),"
                f"recipient({d['Note']})"
            )
        print(self._url)
        response = requests.get(self._url, params=params)
        data = response.json()
        from pprint import pprint
        pprint(params)
        pprint(data)
        df = pd.DataFrame.from_records(data["data"])
        if "Note" in d and "recipient" in d and d["Note"]:
            # matching this in the query to blockchair doesn't work for whatever reason
            try_matching_note = df[df["recipient"] == d["Note"]]
            if not try_matching_note.empty:
                return try_matching_note
        return df


class ETHTransactionFinder(TransactionFinder):
    def __init__(self, key):
        super().__init__(
            currency="ethereum", endpoint="transactions", factor=1e18, key=key
        )


class BCHTransactionFinder(TransactionFinder):
    def __init__(self, key):
        super().__init__(
            currency="bitcoin-cash", endpoint="outputs", factor=1e8, key=key
        )


class BTCTransactionFinder(TransactionFinder):
    def __init__(self, key):
        super().__init__(currency="bitcoin", endpoint="outputs", factor=1e8, key=key)


finders = {
    "BTC": BTCTransactionFinder,
    "BCH": BCHTransactionFinder,
    "ETH": ETHTransactionFinder,
}


def find_transactions(row, key):
    finder = row["transaction_finder"]
    if pd.isnull(finder):
        return pd.DataFrame()
    else:
        result = finder(key=key).find_transactions(row)
        result["Id"] = row["Id"]
        return result


@click.command()
@click.option("--transactions", type=click.Path(exists=True))
@click.option("--blockchair_api_key")
@click.option("--output_file", type=click.Path())
def main(transactions, blockchair_api_key, output_file):
    # transactions -> portfolio state per currency
    # this loads the transactions file and appends columns to the dataframe
    # (one column per currency in the transactions history).
    # These columns represent the state of the portfolio after the transaction in
    # that row is complete.
    # The last row is then the final state of the portfolio.
    t = pd.read_csv(transactions, engine="pyarrow", dtype_backend="pyarrow")
    t["Date"] = pd.to_datetime(t["Date"])
    t = t.sort_values("Date", ascending=True)
    pivoted = pd.pivot(t, index="Id", columns=["Currency"], values=["Price"]).fillna(0)
    portfolio_currencies = pivoted.columns.get_level_values("Currency")
    pivoted.columns = pivoted.columns.droplevel(0)
    pivoted = pivoted.cumsum()
    # same precision used in tabella-criptovalute-3.csv
    # avoids rounding artifacts
    pivoted = pivoted.round(decimals=8)
    merged = pd.merge(t, pivoted, on="Id")

    in_out_flow = merged[merged["Type"].isin(["withdraw", "atm_payment"])].copy()

    in_out_flow["transaction_finder"] = in_out_flow["Currency"].map(finders)
    in_out_flow = in_out_flow.apply(find_transactions, axis=1, key=blockchair_api_key)
    in_out_flow = pd.concat(in_out_flow.tolist())[
        ["transaction_hash", "time", "value", "recipient", "Id"]
    ]
    complete_flow = pd.merge(
        merged[merged["Type"].isin(["withdraw", "atm_payment"])],
        in_out_flow,
        how="left",
        on="Id",
    )
    print(complete_flow)
    complete_flow.to_csv(output_file)


if __name__ == "__main__":
    main()
