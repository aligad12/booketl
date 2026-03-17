"""
BookETL Pipeline
Run with:  python pipeline.py
"""
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def main():
    from scraper.books_scraper import BooksScraper
    from transform.cleaner import clean
    from loader.snowflake_loader import load

    # Step 1 — Scrape
    logger.info("STEP 1/3 — Scraping books.toscrape.com")
    books = BooksScraper().scrape_all()
    logger.info("Scraped %d books", len(books))

    if not books:
        logger.error("No books scraped. Exiting.")
        sys.exit(1)

    # Step 2 — Clean
    logger.info("STEP 2/3 — Cleaning data")
    df = clean(books)
    logger.info("Clean complete: %d rows", len(df))

    # Step 3 — Load
    logger.info("STEP 3/3 — Loading into Snowflake")
    rows = load(df)
    logger.info("Pipeline complete — %d rows in Snowflake", rows)


if __name__ == "__main__":
    main()
