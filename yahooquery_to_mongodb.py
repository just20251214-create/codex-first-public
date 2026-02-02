#!/usr/bin/env python3
"""Fetch all US equity symbols with yahooquery and store company data in MongoDB."""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import math
import os
from typing import Iterable, List

from pymongo import MongoClient, UpdateOne
from yahooquery import Ticker, misc

LOGGER = logging.getLogger(__name__)

SCREENER_URL = "https://query2.finance.yahoo.com/v1/finance/screener"
DEFAULT_MODULES = [
    "assetProfile",
    "summaryProfile",
    "quoteType",
    "price",
    "summaryDetail",
    "defaultKeyStatistics",
    "financialData",
]


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def fetch_us_equity_symbols(page_size: int, max_symbols: int | None) -> List[str]:
    """Fetch US equity symbols using Yahoo Finance screener."""
    symbols: List[str] = []
    offset = 0

    while True:
        if max_symbols is not None and len(symbols) >= max_symbols:
            LOGGER.info("Reached max symbol limit: %s", max_symbols)
            break

        payload = {
            "size": page_size,
            "offset": offset,
            "sortField": "marketCap",
            "sortType": "DESC",
            "quoteType": "EQUITY",
            "query": {
                "operator": "AND",
                "operands": [
                    {"operator": "EQ", "operands": ["region", "us"]},
                ],
            },
        }

        response = misc._make_request(
            SCREENER_URL,
            response_field="finance",
            country="united states",
            method="post",
            data=payload,
        )
        results = response.get("result", [])
        if not results:
            break

        quotes = results[0].get("quotes", [])
        if not quotes:
            break

        new_symbols = [quote["symbol"] for quote in quotes if "symbol" in quote]
        symbols.extend(new_symbols)

        LOGGER.info("Fetched %s symbols (offset %s)", len(symbols), offset)

        if len(quotes) < page_size:
            break

        offset += page_size

    if max_symbols is not None:
        return symbols[:max_symbols]
    return symbols


def upsert_company_data(
    collection,
    symbols: List[str],
    modules: List[str],
    batch_size: int,
) -> None:
    """Fetch company data via yahooquery and upsert into MongoDB."""
    total_batches = math.ceil(len(symbols) / batch_size) if symbols else 0
    for batch_index, batch_symbols in enumerate(chunked(symbols, batch_size), start=1):
        LOGGER.info("Fetching batch %s/%s", batch_index, total_batches)
        ticker = Ticker(batch_symbols, asynchronous=True)
        data = ticker.get_modules(modules)

        operations = []
        timestamp = dt.datetime.utcnow()

        for symbol in batch_symbols:
            symbol_data = data.get(symbol, {})
            operations.append(
                UpdateOne(
                    {"symbol": symbol},
                    {
                        "$set": {
                            "symbol": symbol,
                            "last_updated": timestamp,
                            "data": symbol_data,
                        }
                    },
                    upsert=True,
                )
            )

        if operations:
            result = collection.bulk_write(operations, ordered=False)
            LOGGER.info(
                "Upserted %s documents (matched %s, modified %s)",
                result.upserted_count,
                result.matched_count,
                result.modified_count,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch all US equity symbols from Yahoo Finance and store company data in MongoDB."
    )
    parser.add_argument(
        "--mongodb-uri",
        default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI (default: env MONGODB_URI or mongodb://localhost:27017)",
    )
    parser.add_argument(
        "--mongodb-db",
        default=os.getenv("MONGODB_DB", "market_data"),
        help="MongoDB database name (default: env MONGODB_DB or market_data)",
    )
    parser.add_argument(
        "--mongodb-collection",
        default=os.getenv("MONGODB_COLLECTION", "us_equities"),
        help="MongoDB collection name (default: env MONGODB_COLLECTION or us_equities)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("BATCH_SIZE", "50")),
        help="Number of symbols to fetch per yahooquery batch (default: env BATCH_SIZE or 50)",
    )
    parser.add_argument(
        "--screener-page-size",
        type=int,
        default=int(os.getenv("SCREENER_PAGE_SIZE", "250")),
        help="Number of symbols to request per screener page (default: env SCREENER_PAGE_SIZE or 250)",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None if os.getenv("MAX_SYMBOLS") is None else int(os.getenv("MAX_SYMBOLS")),
        help="Optional limit for number of symbols to process (default: env MAX_SYMBOLS or unlimited)",
    )
    parser.add_argument(
        "--modules",
        default=",".join(DEFAULT_MODULES),
        help="Comma-separated list of yahooquery quoteSummary modules to fetch",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    modules = [module.strip() for module in args.modules.split(",") if module.strip()]

    client = MongoClient(args.mongodb_uri)
    collection = client[args.mongodb_db][args.mongodb_collection]
    collection.create_index("symbol", unique=True)

    symbols = fetch_us_equity_symbols(args.screener_page_size, args.max_symbols)
    if not symbols:
        LOGGER.warning("No symbols found from screener.")
        return

    upsert_company_data(collection, symbols, modules, args.batch_size)


if __name__ == "__main__":
    main()
