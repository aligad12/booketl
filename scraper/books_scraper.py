import logging
import re
import time
from dataclasses import dataclass
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

from config import settings

logger = logging.getLogger(__name__)

RATING_MAP = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


@dataclass
class Book:
    title:    str
    price:    float
    rating:   int
    in_stock: bool
    category: str
    url:      str


class BooksScraper:
    """
    Scrapes all books from books.toscrape.com by category.
    First gets all category URLs from the homepage sidebar,
    then scrapes each category page by page.
    This is the only reliable way to get correct category names.
    """

    def __init__(self):
        self.base_url  = settings.scrape_base_url
        self.session   = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a URL and return BeautifulSoup, or None on failure."""
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml")
        except requests.RequestException as e:
            logger.error("Failed to fetch %s: %s", url, e)
            return None

    def get_categories(self) -> List[dict]:
        """
        Scrape the homepage sidebar to get all category names and URLs.
        Returns a list of {"name": ..., "url": ...} dicts.
        """
        soup = self._get(self.base_url)
        if not soup:
            return []

        categories = []
        for link in soup.select("ul.nav-list > li > ul > li > a"):
            name = link.get_text(strip=True)
            path = link["href"].strip()
            url  = f"{self.base_url}/{path}"
            categories.append({"name": name, "url": url})

        logger.info("Found %d categories", len(categories))
        return categories

    def scrape_category(self, category_name: str, start_url: str) -> List[Book]:
        """Scrape all pages of a single category."""
        books = []
        url   = start_url

        while url:
            soup = self._get(url)
            if not soup:
                break

            for item in soup.select("article.product_pod"):
                book = self._parse_book(item, category_name)
                if book:
                    books.append(book)

            next_btn = soup.select_one("li.next > a")
            if next_btn:
                base_path = url.rsplit("/", 1)[0]
                url = f"{base_path}/{next_btn['href']}"
                time.sleep(1)
            else:
                break

        return books

    def scrape_all(self) -> List[Book]:
        """Scrape every category and return all books."""
        categories = self.get_categories()
        if not categories:
            logger.error("No categories found — check the site is reachable")
            return []

        all_books = []
        for i, cat in enumerate(categories, 1):
            logger.info("Scraping category %d/%d: %s", i, len(categories), cat["name"])
            books = self.scrape_category(cat["name"], cat["url"])
            logger.info("  -> %d books", len(books))
            all_books.extend(books)
            time.sleep(1)

        logger.info("Scraped %d books total across %d categories",
                    len(all_books), len(categories))
        return all_books

    def _parse_book(self, item, category: str) -> Optional[Book]:
        """Extract data from one book HTML element."""
        try:
            title_tag  = item.select_one("h3 a")
            title      = title_tag["title"] if title_tag else "Unknown"

            price_tag  = item.select_one("p.price_color")
            price_text = price_tag.get_text(strip=True) if price_tag else "0"
            price      = float(re.sub(r"[^\d.]", "", price_text) or "0")

            rating_tag  = item.select_one("p.star-rating")
            rating_word = rating_tag["class"][1] if rating_tag else "One"
            rating      = RATING_MAP.get(rating_word, 0)

            stock_tag = item.select_one("p.availability")
            in_stock  = "in stock" in stock_tag.get_text(strip=True).lower() \
                        if stock_tag else False

            href     = title_tag["href"] if title_tag else ""
            full_url = f"{self.base_url}/catalogue/{href.replace('../', '')}"

            return Book(
                title=title,
                price=price,
                rating=rating,
                in_stock=in_stock,
                category=category,
                url=full_url,
            )

        except Exception as e:
            logger.warning("Could not parse book: %s", e)
            return None