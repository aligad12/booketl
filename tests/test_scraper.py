from unittest.mock import patch, MagicMock
from scraper.books_scraper import BooksScraper

SAMPLE_HTML = """
<html><body>
<article class="product_pod">
  <h3><a href="../mystery/book-one_100/index.html" title="Book One">Book One</a></h3>
  <p class="price_color">£12.50</p>
  <p class="star-rating Four"></p>
  <p class="availability">In stock</p>
</article>
</body></html>
"""


class TestBooksScraper:

    def setup_method(self):
        self.scraper = BooksScraper()

    @patch("scraper.books_scraper.requests.Session.get")
    def test_scrape_page_returns_books(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_HTML
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        books = self.scraper._scrape_page(1)

        assert len(books) == 1
        assert books[0].title == "Book One"
        assert books[0].price == 12.50
        assert books[0].rating == 4
        assert books[0].in_stock is True

    @patch("scraper.books_scraper.requests.Session.get")
    def test_http_error_returns_empty(self, mock_get):
        import requests
        mock_get.side_effect = requests.RequestException("timeout")

        books = self.scraper._scrape_page(1)
        assert books == []

    @patch("scraper.books_scraper.requests.Session.get")
    def test_empty_page_returns_empty(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.text = "<html><body></body></html>"
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        books = self.scraper._scrape_page(99)
        assert books == []
