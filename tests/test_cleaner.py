import pandas as pd
from transform.cleaner import clean
from scraper.books_scraper import Book


class TestCleaner:

    def test_returns_dataframe(self, sample_books):
        df = clean(sample_books)
        assert isinstance(df, pd.DataFrame)

    def test_deduplicates(self, sample_books):
        df = clean(sample_books)
        assert len(df) == 3  # 4 input, 1 duplicate removed

    def test_price_band_exists(self, sample_books):
        df = clean(sample_books)
        assert "price_band" in df.columns

    def test_is_highly_rated(self, sample_books):
        df = clean(sample_books)
        assert df[df["rating"] >= 4]["is_highly_rated"].all()
        assert not df[df["rating"] < 4]["is_highly_rated"].any()

    def test_scraped_at_exists(self, sample_books):
        df = clean(sample_books)
        assert "scraped_at" in df.columns
        assert df["scraped_at"].notna().all()

    def test_removes_zero_price(self):
        books = [
            Book("Good",  15.00, 3, True,  "Fiction", "http://x.com/1"),
            Book("Bad",    0.0,  3, True,  "Fiction", "http://x.com/2"),
            Book("Bad2",  -5.0,  3, True,  "Fiction", "http://x.com/3"),
        ]
        df = clean(books)
        assert len(df) == 1
        assert df.iloc[0]["title"] == "Good"
