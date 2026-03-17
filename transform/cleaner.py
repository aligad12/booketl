import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd

from scraper.books_scraper import Book

logger = logging.getLogger(__name__)


def clean(books: List[Book]) -> pd.DataFrame:
    if not books:
        raise ValueError("No books to clean.")

    df = pd.DataFrame([{
        "title":    b.title,
        "price":    b.price,
        "rating":   b.rating,
        "in_stock": b.in_stock,
        "category": b.category,
        "url":      b.url,
    } for b in books])

    logger.info("Starting clean — %d rows", len(df))

    # Deduplicate
    df = df.drop_duplicates(subset=["title", "price"])

    # Validate
    df = df[df["title"].notna() & (df["title"] != "Unknown")]
    df = df[(df["price"] > 0) & (df["price"] < 1000)]

    # Derived columns
    df["price_band"] = pd.cut(
        df["price"],
        bins=[0, 10, 20, 35, 60, 1000],
        labels=["Under 10", "10 to 20", "20 to 35", "35 to 60", "Over 60"],
    ).astype(str)

    df["is_highly_rated"] = df["rating"] >= 4
    df["scraped_at"]      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Types
    df["price"]    = df["price"].astype(float).round(2)
    df["rating"]   = df["rating"].astype(int)
    df["in_stock"] = df["in_stock"].astype(bool)

    df = df.reset_index(drop=True)
    logger.info("Clean complete — %d rows", len(df))
    return df
