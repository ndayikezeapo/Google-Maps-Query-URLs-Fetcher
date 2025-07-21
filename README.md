# Apollinaire
Google Maps Query URLs Fetcher
This project provides a Python-based, open-source system designed to generate Google Maps search queries, scrape resulting place URLs, and store them efficiently. It leverages asynchronous programming (asyncio) and headless browser automation (Playwright) to achieve high throughput.
IMPORTANT DISCLAIMER: Please be aware that automated scraping of websites, especially at high volumes, may violate the Terms of Service of the target website (e.g., Google Maps). Google actively employs anti-bot measures, and attempting to scrape at the scale of 1,000 unique links per minute without using official APIs or proxies is highly likely to result in IP blocking, CAPTCHAs, or other restrictions. This project is provided for educational and theoretical purposes only. Users are responsible for ensuring their activities comply with all applicable laws and website terms of service.
Features
•	Query Generation: Automatically creates Google Maps search URLs from lists of industry keywords and ZIP codes.
•	Headless Browsing: Uses Playwright to navigate and interact with Google Maps pages in a headless (non-visual) browser environment.
•	Link Extraction: Identifies and extracts unique Google Maps place URLs from search results.
•	De-duplication: Ensures no duplicate URLs are stored, both in-memory during a run and persistently across runs using an SQLite database.
•	Persistent Storage: Stores unique URLs with timestamps in a local SQLite database.
•	High Concurrency: Utilizes asyncio to parallelize fetching and processing, maximizing throughput.
•	Error Handling: Implements retries with exponential backoff for failed requests, allowing the scraper to continue processing.
•	Configurable: Parameters like concurrency, timeouts, and batch sizes can be easily adjusted.
•	Performance Monitoring: Logs the rate of unique URLs processed per minute.
Technical Stack
•	Language: Python 3.10+
•	Automation: Playwright (headless Chromium)
•	Parsing: Playwright selectors (JavaScript execution in browser context)
•	Concurrency: asyncio
•	Storage: sqlite3
•	Logging: Python's built-in logging module
Architecture & Data Flow
graph LR
    A[4K Industries] & B[42K ZIPs] --> G[Query Generator]
    G --> Q{Batched URLs Queue}
    Q --> F[Fetcher (Playwright Worker Pool)]
    F --> P[Link Extractor]
    P --> D[De-duplication (In-Memory Set & SQLite)]
    D --> S[Storage (SQLite)]
    F --> E[Error Handling & Retry Queue]
    E --> Q
    F --> L[Logs]
    D --> L
    S --> L

Setup & Run Instructions
Prerequisites
•	Python 3.10 or higher
•	pip package manager
•	Docker (optional, but highly recommended for isolated and consistent environments)
Local Setup
1.	Create Project Directory:
2.	mkdir google_maps_scraper
3.	cd google_maps_scraper

4.	Create scraper.py: Save the Python code provided above into a file named scraper.py within this directory.
5.	Create requirements.txt: Create a file named requirements.txt in the same directory with the following content:
6.	playwright==1.45.0

7.	Install Python Dependencies:
8.	pip install -r requirements.txt

9.	Install Playwright Browsers: Playwright needs to download the browser binaries.
10.	playwright install chromium

11.	Prepare Input Files: Create two plain text files in the same directory as scraper.py:
o	industries.txt: One industry keyword per line.
o	restaurant
o	hotel
o	dentist
o	... (your 4,000 keywords)

o	zip_codes.txt: One ZIP code per line.
o	90210
o	10001
o	30303
o	... (your 42,000 ZIP codes)

12.	(The scraper.py script will create dummy versions of these files if they don't exist for initial testing.)
13.	Run the Scraper:
14.	python scraper.py

Docker Deployment (Recommended for Production)
Using Docker provides a consistent environment and simplifies deployment.
1.	Create Dockerfile: Create a file named Dockerfile in the project root with the following content:
2.	# Use an official Python runtime as a parent image
3.	FROM python:3.10-slim-buster
4.	
5.	# Install system dependencies required by Playwright
6.	RUN apt-get update && apt-get install -y \
7.	    libnss3 \
8.	    libxss1 \
9.	    libasound2 \
10.	    libatk-bridge2.0-0 \
11.	    libgtk-3-0 \
12.	    libgdk-pixbuf2.0-0 \
13.	    libffi-dev \
14.	    libreadline-dev \
15.	    libsqlite3-dev \
16.	    --no-install-recommends \
17.	    && rm -rf /var/lib/apt/lists/*
18.	
19.	# Set the working directory inside the container
20.	WORKDIR /app
21.	
22.	# Copy requirements file and install Python dependencies
23.	COPY requirements.txt .
24.	RUN pip install --no-cache-dir -r requirements.txt
25.	
26.	# Install Playwright browsers (Chromium)
27.	# Set PLAYWRIGHT_BROWSERS_PATH to store browsers in a persistent volume if needed
28.	ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
29.	RUN playwright install chromium
30.	
31.	# Copy the rest of the application code
32.	COPY . .
33.	
34.	# Command to run the application
35.	CMD ["python", "scraper.py"]

36.	Build Docker Image: Navigate to your project directory in the terminal and build the image:
37.	docker build -t google-maps-scraper .

38.	Run Docker Container:
39.	docker run -v "$(pwd)":/app --name maps_scraper google-maps-scraper

o	-v "$(pwd)":/app: This mounts your current host directory to /app inside the container. This allows the container to read your industries.txt and zip_codes.txt and write the google_maps_urls.db and scraper.log files back to your host machine.
o	--name maps_scraper: Assigns a name to your container for easy management.
Configuration
All configurable parameters are located in the CONFIG dictionary at the top of scraper.py. You can adjust these values to fine-tune performance and resource usage:
•	INDUSTRIES_FILE, ZIP_CODES_FILE, OUTPUT_DB, LOG_FILE: Paths for input/output files.
•	MAX_CONCURRENT_BROWSERS: Determines the number of simultaneous browser instances (Playwright contexts). Increasing this can boost concurrency but also increases resource consumption and detection risk.
•	MAX_CONCURRENT_PAGES_PER_BROWSER: Number of pages each browser context can manage. For Google Maps, keeping this at 1 is generally safer.
•	PAGE_LOAD_TIMEOUT_MS: The maximum time (in milliseconds) to wait for a page to load or for specific elements to appear.
•	RETRY_ATTEMPTS, RETRY_BACKOFF_FACTOR: Control how many times a failed query is retried and the delay between retries.
•	BATCH_SIZE_DB_WRITE: The number of unique URLs collected in memory before a batch write operation to the SQLite database is performed. Larger batches reduce I/O overhead.
•	GOOGLE_MAPS_BASE_URL: The base URL for constructing search queries.
•	TARGET_URLS_PER_MINUTE: A target value for performance logging.
•	MONITOR_INTERVAL_SECONDS: How frequently the scraper logs its current unique URLs per minute rate.
•	USER_AGENT: Sets the User-Agent header for Playwright requests to mimic a real browser.
•	HEADLESS: Set to True for server deployment (no browser UI), False for debugging (browser UI visible).
Output
After running the scraper, you will find the following files in your project directory:
•	google_maps_urls.db: This is an SQLite database file. You can use any SQLite browser (e.g., DB Browser for SQLite) to open it and view the urls table, which contains:
o	url: The unique Google Maps place URL.
o	timestamp: The time when the URL was first added to the database.
•	scraper.log: This log file records all significant events, including:
o	Initialization messages.
o	URLs being fetched and extracted.
o	Errors encountered (e.g., timeouts, network issues).
o	Performance metrics: Entries like Performance: X.XX unique URLs/minute. Total unique processed: YYY. These lines indicate the achieved throughput over the last MONITOR_INTERVAL_SECONDS.
Performance Summary & Acceptance Criteria
The primary acceptance criterion is demonstrating sustained ≥1,000 unique Google Maps place URLs/min.
How to interpret performance: The scraper.log file will provide continuous updates on the Performance: X.XX unique URLs/minute metric. This value represents the average rate of new, unique URLs successfully added to the database during the last monitoring interval.
Challenges to achieving 1,000 links/min: As mentioned in the disclaimer, reaching and sustaining 1,000 unique links per minute without paid proxies or advanced anti-bot bypass techniques is extremely challenging due to Google's protective measures. You may observe:
•	Fluctuating rates: Performance might start high but then drop significantly as Google detects and throttles requests.
•	CAPTCHAs: Playwright might encounter CAPTCHA challenges, which this script does not automatically solve (requiring human intervention or a paid CAPTCHA solving service).
•	IP Blocking: Your server's IP address might get temporarily or permanently blocked, leading to TimeoutError or other connection errors.
To optimize for throughput, you will need to:
1.	Tune MAX_CONCURRENT_BROWSERS and PAGE_LOAD_TIMEOUT_MS: Experiment with these values. Too high, and you risk detection; too low, and you won't reach the target.
2.	Monitor Logs Closely: Look for patterns in errors (e.g., frequent timeouts, specific error messages) that might indicate throttling or blocking.
3.	Refine Selectors: Google's HTML changes. If the extraction rate drops or you see many "No place links found" warnings, inspect the live Google Maps page to update the Playwright selectors in fetch_and_extract.
This system provides the foundation for a high-throughput scraper. Achieving the ambitious target without external services will require continuous monitoring, adaptation, and potentially more sophisticated anti-bot techniques than can be provided in a simple open-source solution.
