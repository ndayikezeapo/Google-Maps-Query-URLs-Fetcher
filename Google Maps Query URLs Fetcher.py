import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime
from collections import deque # Not explicitly used for rate limiting here, but useful for advanced scenarios

from playwright.async_api import async_playwright, Playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError

# --- Configuration ---
# All configurable parameters for the scraper.
# Adjust these values to tune performance and resource usage.
CONFIG = {
    "INDUSTRIES_FILE": "industries.txt",  # Path to the file containing industry keywords
    "ZIP_CODES_FILE": "zip_codes.txt",    # Path to the file containing ZIP codes
    "OUTPUT_DB": "google_maps_urls.db",   # SQLite database file to store unique URLs
    "LOG_FILE": "scraper.log",            # Log file for all system events and performance metrics
    "MAX_CONCURRENT_BROWSERS": 5,         # Number of concurrent Playwright browser contexts (isolated instances)
    "MAX_CONCURRENT_PAGES_PER_BROWSER": 1, # Number of pages each browser context can handle.
                                          # For Google Maps, 1 is safer to avoid detection.
    "PAGE_LOAD_TIMEOUT_MS": 45000,        # Timeout for page navigation and loading (in milliseconds)
    "RETRY_ATTEMPTS": 3,                  # Number of times to retry a failed URL fetch
    "RETRY_BACKOFF_FACTOR": 2,            # Exponential backoff factor (e.g., 1, 2, 4 seconds between retries)
    "BATCH_SIZE_DB_WRITE": 500,           # Number of unique URLs to accumulate before batch writing to DB
    "GOOGLE_MAPS_BASE_URL": "https://www.google.com/maps/search/", # Base URL for Google Maps search queries
    "TARGET_URLS_PER_MINUTE": 1000,       # Target for performance monitoring and reporting
    "MONITOR_INTERVAL_SECONDS": 10,       # How often to log performance metrics (in seconds)
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "HEADLESS": True,                     # Run Playwright in headless mode (True for server deployment)
}

# --- Logging Setup ---
# Configure the logging system to output to both a file and the console.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG["LOG_FILE"]),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Global State for De-duplication and Performance Monitoring ---
# Using a set for efficient in-memory de-duplication within the current run.
unique_urls_processed = set()
# Counters for performance metrics.
processed_count_this_interval = 0
last_monitor_time = time.time()
# Database connection and cursor.
db_connection = None
db_cursor = None
# List to hold URLs pending batch write to the database.
pending_db_writes = []
# Queue to hold browser contexts for reuse by workers.
browser_context_queue: asyncio.Queue = asyncio.Queue()

# --- Database Operations ---
def init_db():
    """Initializes the SQLite database and loads existing URLs for de-duplication."""
    global db_connection, db_cursor
    try:
        db_connection = sqlite3.connect(CONFIG["OUTPUT_DB"])
        db_cursor = db_connection.cursor()
        # Optimize SQLite for faster writes. WAL mode is generally better for concurrency.
        db_cursor.execute("PRAGMA journal_mode = WAL;")
        db_cursor.execute("PRAGMA synchronous = OFF;") # Can be set to NORMAL for more safety
        db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                timestamp TEXT
            )
        """)
        db_connection.commit()
        logger.info(f"Database initialized: {CONFIG['OUTPUT_DB']}")

        # Load existing URLs into memory for de-duplication across multiple runs.
        logger.info("Loading existing URLs from database for de-duplication...")
        start_time = time.time()
        for row in db_cursor.execute("SELECT url FROM urls"):
            unique_urls_processed.add(row[0])
        logger.info(f"Loaded {len(unique_urls_processed)} existing URLs in {time.time() - start_time:.2f} seconds.")
    except sqlite3.Error as e:
        logger.critical(f"Failed to initialize database: {e}")
        raise # Re-raise to stop execution if DB cannot be initialized

async def persist_urls_batch():
    """Writes accumulated unique URLs to the database in a batch."""
    global pending_db_writes
    if not pending_db_writes:
        return

    try:
        # Use INSERT OR IGNORE to prevent adding duplicate URLs if they already exist
        # (e.g., if another process added them concurrently or if the in-memory set missed something).
        db_cursor.executemany("INSERT OR IGNORE INTO urls (url, timestamp) VALUES (?, ?)", pending_db_writes)
        db_connection.commit()
        logger.info(f"Successfully wrote {len(pending_db_writes)} URLs to DB.")
        pending_db_writes = []
    except sqlite3.Error as e:
        logger.error(f"Error writing batch to database: {e}. Data might be lost for this batch.")
        # Optionally, log the failed batch for manual recovery or re-queue.
    finally:
        # Clear the batch even on error to prevent infinite retries of a bad batch.
        pending_db_writes = []

async def add_unique_url(url: str) -> bool:
    """
    Adds a URL to the in-memory set and pending DB write list if it's unique.
    Triggers a batch write if the pending list reaches the configured size.
    Returns True if the URL was unique and added, False otherwise.
    """
    global processed_count_this_interval
    if url not in unique_urls_processed:
        unique_urls_processed.add(url)
        timestamp = datetime.now().isoformat()
        pending_db_writes.append((url, timestamp))
        processed_count_this_interval += 1

        if len(pending_db_writes) >= CONFIG["BATCH_SIZE_DB_WRITE"]:
            await persist_urls_batch()
        return True
    return False

# --- Query Generator ---
def load_data(filepath: str) -> list[str]:
    """Loads data (industries or ZIP codes) from a text file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.critical(f"Input file not found: {filepath}. Please ensure it exists.")
        return []
    except Exception as e:
        logger.critical(f"Error loading data from {filepath}: {e}")
        return []

def generate_queries(industries: list[str], zip_codes: list[str]) -> list[str]:
    """
    Generates Google Maps search URLs based on industries and ZIP codes.
    Uses the specified URL format: https://www.google.com/maps/search/<industry>+near+<zipcode>
    """
    queries = []
    for industry in industries:
        for zipcode in zip_codes:
            # Construct the query string as specified: <industry>+near+<zipcode>
            query_string = f"{industry}+near+{zipcode}"
            # URL-encode the query string by replacing spaces with '+'
            encoded_query = query_string.replace(" ", "+")
            queries.append(f"{CONFIG['GOOGLE_MAPS_BASE_URL']}{encoded_query}")
    logger.info(f"Generated {len(queries)} potential queries.")
    return queries

# --- Playwright Fetcher and Link Extractor ---
async def fetch_and_extract(page: Page, url: str, attempt: int = 1) -> bool:
    """
    Fetches a Google Maps search results page and extracts place URLs.
    Returns True on successful extraction, False otherwise.
    """
    try:
        logger.info(f"Fetching: {url} (Attempt {attempt})")
        # Navigate to the URL, waiting until network is idle or specific selector appears.
        # 'networkidle' is often good for initial page load.
        await page.goto(url, wait_until='domcontentloaded', timeout=CONFIG["PAGE_LOAD_TIMEOUT_MS"])

        # IMPORTANT: Google Maps HTML structure is highly dynamic and changes frequently.
        # The selectors below are *examples* and will almost certainly need to be updated
        # based on the current live HTML structure of Google Maps search results.
        # You will need to inspect the page manually using browser developer tools
        # to find reliable selectors for place links (e.g., links with '/maps/place/' in href,
        # or elements with specific data attributes like 'data-item-id').

        # Wait for a common element that indicates results have loaded.
        # This is a generic example; a more specific selector would be better.
        # For example, a div containing the list of results.
        try:
            await page.wait_for_selector('a[href*="/maps/place/"]', timeout=10000) # Wait for at least one place link
        except PlaywrightTimeoutError:
            logger.warning(f"No place links found or page not loaded within 10s for {url}. Continuing extraction.")

        # Extract links. This JavaScript runs in the browser context.
        links = await page.evaluate('''() => {
            const results = [];
            // Common patterns for Google Maps place links:
            // 1. Links directly to a place page: href containing /maps/place/
            // 2. Links within specific result containers (e.g., div[role="feed"] a)
            // 3. Links with specific data attributes (e.g., data-item-id)

            // Attempt 1: Look for direct place links
            document.querySelectorAll('a[href*="/maps/place/"]').forEach(link => {
                results.push(link.href);
            });

            // Attempt 2: Look for links within common result list containers
            // This selector is highly prone to change.
            document.querySelectorAll('div[role="feed"] a').forEach(link => {
                if (link.href && link.href.includes('/maps/place/')) {
                    results.push(link.href);
                }
            });

            // You might need to scroll the page to load more results if they are lazy-loaded
            // For first page, this might not be strictly necessary.

            return [...new Set(results)]; // Return unique links
        }''')

        extracted_count = 0
        for link in links:
            # Basic validation to ensure it's a Google Maps place URL
            if "/maps/place/" in link and "google.com/maps" in link:
                if await add_unique_url(link):
                    extracted_count += 1
        logger.info(f"Extracted {extracted_count} new unique links from {url}")
        return True # Indicates the fetch and extraction process completed, even if no links found
    except PlaywrightTimeoutError:
        logger.warning(f"Timeout fetching or waiting for elements on {url} after {attempt} attempts.")
        return False
    except Exception as e:
        logger.error(f"Error fetching or extracting from {url}: {e}")
        return False

async def worker(query_queue: asyncio.Queue):
    """
    Worker function to process URLs from the queue using a shared Playwright browser context.
    Each worker acquires a browser context from `browser_context_queue`, uses it, and returns it.
    """
    page: Page = None # Initialize page to None
    context = None
    try:
        while True:
            # Get a browser context from the shared pool
            context = await browser_context_queue.get()
            try:
                url = await query_queue.get()
                retries = 0
                success = False
                while retries < CONFIG["RETRY_ATTEMPTS"] and not success:
                    try:
                        # Create a new page within the context for each URL
                        page = await context.new_page()
                        # Set user agent to mimic a real browser
                        await page.set_extra_http_headers({"User-Agent": CONFIG["USER_AGENT"]})
                        success = await fetch_and_extract(page, url, retries + 1)
                    except Exception as e:
                        logger.error(f"Error during page operation for {url}: {e}")
                        success = False # Mark as failed for retry
                    finally:
                        if page:
                            await page.close() # Always close the page to free resources
                            page = None # Reset page variable

                    if not success and retries < CONFIG["RETRY_ATTEMPTS"] - 1:
                        sleep_time = CONFIG["RETRY_BACKOFF_FACTOR"] ** retries
                        logger.info(f"Retrying {url} in {sleep_time} seconds...")
                        await asyncio.sleep(sleep_time)
                    retries += 1

                if not success:
                    logger.error(f"Failed to process {url} after {CONFIG['RETRY_ATTEMPTS']} attempts. Skipping.")
            except asyncio.CancelledError:
                logger.info("Worker cancelled.")
                break # Exit loop if cancelled
            except Exception as e:
                logger.error(f"Unhandled error in worker loop: {e}")
            finally:
                query_queue.task_done()
                # Return the browser context to the pool for reuse
                await browser_context_queue.put(context)
                context = None # Reset context variable
    except Exception as e:
        logger.critical(f"Critical error in worker setup/teardown: {e}")
    finally:
        # Ensure context is returned even if loop breaks unexpectedly
        if context and not browser_context_queue.full():
            await browser_context_queue.put(context)


# --- Performance Monitoring ---
async def performance_monitor():
    """Monitors and logs the rate of unique URLs processed."""
    global processed_count_this_interval, last_monitor_time
    while True:
        await asyncio.sleep(CONFIG["MONITOR_INTERVAL_SECONDS"])
        current_time = time.time()
        elapsed_seconds = current_time - last_monitor_time

        if elapsed_seconds > 0:
            rate_per_minute = (processed_count_this_interval / elapsed_seconds) * 60
            logger.info(f"Performance: {rate_per_minute:.2f} unique URLs/minute. Total unique processed: {len(unique_urls_processed)}")
        else:
            logger.warning("Elapsed time for performance monitoring is zero, skipping rate calculation.")

        processed_count_this_interval = 0 # Reset counter for the next interval
        last_monitor_time = current_time # Reset timer


# --- Main Execution ---
async def main():
    """Main function to orchestrate the scraping process."""
    init_db()

    industries = load_data(CONFIG["INDUSTRIES_FILE"])
    zip_codes = load_data(CONFIG["ZIP_CODES_FILE"])

    if not industries or not zip_codes:
        logger.critical("Industry or ZIP code data not loaded. Exiting.")
        if db_connection:
            db_connection.close()
        return

    all_queries = generate_queries(industries, zip_codes)
    # Shuffle queries to distribute load and potentially avoid immediate IP blocks
    import random
    random.shuffle(all_queries)

    query_queue = asyncio.Queue()
    for query in all_queries:
        await query_queue.put(query)
    logger.info(f"All {len(all_queries)} queries added to queue.")

    # Playwright setup
    playwright_instance: Playwright = None
    try:
        playwright_instance = await async_playwright().start()
        # Launch multiple browser contexts and add them to the pool queue
        for i in range(CONFIG["MAX_CONCURRENT_BROWSERS"]):
            browser = await playwright_instance.chromium.launch(headless=CONFIG["HEADLESS"])
            context = await browser.new_context()
            await browser_context_queue.put(context)
            logger.info(f"Launched browser context {i+1}/{CONFIG['MAX_CONCURRENT_BROWSERS']} and added to pool.")

        # Create worker tasks. The number of workers should ideally match the number of contexts
        # or be slightly higher if contexts can handle multiple pages (but we set pages per browser to 1).
        num_workers = CONFIG["MAX_CONCURRENT_BROWSERS"] * CONFIG["MAX_CONCURRENT_PAGES_PER_BROWSER"]
        workers = [
            asyncio.create_task(worker(query_queue))
            for _ in range(num_workers)
        ]
        logger.info(f"Started {num_workers} worker tasks.")

        # Start performance monitor
        monitor_task = asyncio.create_task(performance_monitor())

        # Wait for all queries to be processed
        await query_queue.join()
        logger.info("All queries processed.")

        # Cancel workers and monitor task
        for worker_task in workers:
            worker_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True) # Await cancellation and gather results
        monitor_task.cancel()
        await monitor_task # Await monitor task cancellation

        # Persist any remaining pending writes to the database
        await persist_urls_batch()

    except Exception as e:
        logger.critical(f"Critical error during main execution: {e}")
    finally:
        # Close all browser contexts in the pool
        while not browser_context_queue.empty():
            context_to_close = await browser_context_queue.get()
            try:
                await context_to_close.browser.close()
                logger.info("Closed a browser context.")
            except Exception as e:
                logger.error(f"Error closing browser context: {e}")
        
        # Stop Playwright instance
        if playwright_instance:
            await playwright_instance.stop()
            logger.info("Playwright instance stopped.")

        # Close database connection
        if db_connection:
            db_connection.close()
            logger.info("Database connection closed.")
        logger.info("Scraping process completed.")

if __name__ == "__main__":
    # Create dummy input files for testing if they don't exist.
    # In a real scenario, you would upload your 4000 industries and 42000 ZIP codes.
    if not os.path.exists(CONFIG["INDUSTRIES_FILE"]):
        logger.warning(f"'{CONFIG['INDUSTRIES_FILE']}' not found. Creating dummy file.")
        with open(CONFIG["INDUSTRIES_FILE"], "w") as f:
            f.write("restaurant\nhotel\ndentist\nmechanic\n")
    if not os.path.exists(CONFIG["ZIP_CODES_FILE"]):
        logger.warning(f"'{CONFIG['ZIP_CODES_FILE']}' not found. Creating dummy file.")
        with open(CONFIG["ZIP_CODES_FILE"], "w") as f:
            f.write("90210\n10001\n30303\n75001\n")

    asyncio.run(main())
