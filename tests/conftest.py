import pytest
from scraper.books_scraper import Book


@pytest.fixture
def sample_books():
    return [
        Book(title="Book One",   price=12.50, rating=4,
             in_stock=True,  category="Mystery", url="http://example.com/1"),
        Book(title="Book Two",   price=7.99,  rating=2,
             in_stock=False, category="Fiction", url="http://example.com/2"),
        Book(title="Book Three", price=24.00, rating=5,
             in_stock=True,  category="Mystery", url="http://example.com/3"),
        Book(title="Book One",   price=12.50, rating=4,
             in_stock=True,  category="Mystery", url="http://example.com/1"),
        # ^^^ duplicate of the first — dedup test should reduce count to 3
    ]
