import requests
import csv
import json
import time
import re
import schedule
import logging
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


class DeFiLlamaScraper:
    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.config = self.load_config()
        self.setup_logging()
        self.current_proxy_index = 0
        self.proxy_session = None
        self.setup_proxy_session()

    def load_config(self):
        default_config = {
            "scrape_interval_minutes": 5,
            "output_filename": "defillama_chains.csv",
            "log_filename": "defillama_scraper.log",
            "max_retries": 3,
            "retry_delay_seconds": 10,
            "enable_logging": True,
            "log_level": "INFO",
            "save_historical_data": False,
            "historical_data_dir": "historical_data",
            "include_zero_tvl": True,
            "proxy": {
                "enabled": False,
                "type": "http",
                "host": "",
                "port": 8080,
                "username": "",
                "password": "",
                "rotate_proxies": False,
                "proxy_list": [],
                "rotation_interval": 10
            }
        }

        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, "r") as f:
                    loaded_config = json.load(f)
                default_config.update(loaded_config)
                print(f"Configuration loaded from {self.config_file}")
            else:
                with open(self.config_file, "w") as f:
                    json.dump(default_config, f, indent=4)
                print(f"Default configuration created: {self.config_file}")

            return default_config

        except Exception as e:
            print(f"Error loading config: {e}")
            print("Using default configuration")
            return default_config

    def setup_logging(self):
        if not self.config["enable_logging"]:
            logging.disable(logging.CRITICAL)
            return

        log_level = getattr(logging, self.config["log_level"].upper(), logging.INFO)

        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.config["log_filename"]),
                logging.StreamHandler()
            ]
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info("DeFiLlama Scraper initialized")

    def setup_proxy_session(self):
        self.proxy_session = requests.Session()

        proxy_config = self.config.get("proxy", {})
        if not proxy_config.get("enabled", False):
            self.logger.info("Proxy disabled")
            return

        try:
            proxy_info = self.get_current_proxy()
            if proxy_info:
                self.proxy_session.proxies.update(proxy_info)
                self.logger.info(f"Proxy configured: {proxy_info}")
            else:
                self.logger.warning("Proxy enabled but no valid proxy configuration found")
        except Exception as e:
            self.logger.error(f"Error setting up proxy: {e}")

    def get_current_proxy(self):
        proxy_config = self.config.get("proxy", {})

        if not proxy_config.get("enabled", False):
            return None

        if proxy_config.get("rotate_proxies", False):
            proxy_list = proxy_config.get("proxy_list", [])
            if isinstance(proxy_list, list) and proxy_list:
                current_proxy = proxy_list[self.current_proxy_index % len(proxy_list)]
                return self.format_proxy_dict(current_proxy)
            else:
                self.logger.warning("Proxy rotation is enabled, but 'proxy_list' is missing or invalid.")

        if proxy_config.get("host"):
            return self.format_proxy_dict(proxy_config)

        return None

    def format_proxy_dict(self, proxy_config):
        try:
            proxy_type = proxy_config.get("type", "http").lower()
            host = proxy_config.get("host", "")
            port = proxy_config.get("port", 8080)
            username = proxy_config.get("username", "")
            password = proxy_config.get("password", "")

            if not host:
                return None

            if username and password:
                proxy_url = f"{proxy_type}://{username}:{password}@{host}:{port}"
            else:
                proxy_url = f"{proxy_type}://{host}:{port}"

            if proxy_type in ["http", "https", "socks5", "socks4"]:
                return {"http": proxy_url, "https": proxy_url}
            else:
                self.logger.warning(f"Unsupported proxy type: {proxy_type}")
                return None

        except Exception as e:
            self.logger.error(f"Error formatting proxy: {e}")
            return None

    def get_chains_data_api(self):
        try:
            self.logger.info("Fetching chains data from DeFiLlama API...")

            chains_url = "https://api.llama.fi/v2/chains"
            response = self.proxy_session.get(chains_url, timeout=30)
            response.raise_for_status()
            chains_data = response.json()

            self.logger.info(f"Found {len(chains_data)} chains")

            self.logger.info("Fetching protocols data to count protocols per chain...")
            protocols_url = "https://api.llama.fi/protocols"
            response = self.proxy_session.get(protocols_url, timeout=30)
            response.raise_for_status()
            protocols_data = response.json()

            protocol_counts = defaultdict(int)
            for protocol in protocols_data:
                chains = protocol.get("chains", [])
                if isinstance(chains, list):
                    for chain in chains:
                        protocol_counts[chain] += 1

            self.logger.info(f"Counted protocols for {len(protocol_counts)} chains")

            csv_data = []
            include_zero_tvl = self.config.get("include_zero_tvl", True)

            for chain in chains_data:
                name = chain.get("name", "Unknown")
                tvl = chain.get("tvl", 0)
                protocols = protocol_counts.get(name, 0)

                if name and (include_zero_tvl or tvl > 0):
                    csv_data.append({
                        "name": name,
                        "protocols": protocols,
                        "tvl": round(tvl, 2),
                        "timestamp": datetime.now().isoformat()
                    })

            self.logger.info(f"Filtered {len(csv_data)} chains (include_zero_tvl: {include_zero_tvl})")

            zero_tvl_count = sum(1 for chain in csv_data if chain["tvl"] == 0)
            non_zero_tvl_count = len(csv_data) - zero_tvl_count
            self.logger.info(f"Chains with TVL > 0: {non_zero_tvl_count}")
            self.logger.info(f"Chains with TVL = 0: {zero_tvl_count}")

            return csv_data

        except Exception as e:
            self.logger.error(f"API method failed: {e}")
            return None

    def get_chains_data_selenium(self):
        self.logger.info("Using Selenium to scrape DeFiLlama...")

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        proxy_config = self.config.get("proxy", {})
        if proxy_config.get("enabled", False):
            proxy_info = self.get_current_proxy()
            if proxy_info:
                proxy_url = proxy_info.get("http", "")
                if proxy_url:
                    import urllib.parse
                    parsed = urllib.parse.urlparse(proxy_url)

                    if parsed.hostname and parsed.port:
                        chrome_options.add_argument(f"--proxy-server={parsed.hostname}:{parsed.port}")
                        self.logger.info(f"Selenium using proxy: {parsed.hostname}:{parsed.port}")

                        if parsed.username and parsed.password:
                            self.logger.warning(
                                "Selenium proxy authentication may require additional setup"
                            )

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            driver.get("https://defillama.com/chains")

            WebDriverWait(driver, 20).until(
                ec.presence_of_element_located((By.TAG_NAME, "body"))
            )

            time.sleep(5)

            chains_data = []
            include_zero_tvl = self.config.get("include_zero_tvl", True)

            try:
                rows = driver.find_elements(By.TAG_NAME, "tr")
                self.logger.info(f"Found {len(rows)} table rows")

                for row in rows[1:]:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 3:
                            name = cells[0].text.strip()
                            protocols_text = cells[1].text.strip()
                            tvl_text = cells[2].text.strip()

                            if name and (include_zero_tvl or (
                                    "$" in tvl_text and not tvl_text.strip() in ["$0", "$0.00", "-"])):
                                protocols = self.extract_number(protocols_text)
                                tvl = self.extract_tvl(tvl_text) if "$" in tvl_text else 0

                                chains_data.append({
                                    "name": name,
                                    "protocols": protocols,
                                    "tvl": tvl,
                                    "timestamp": datetime.now().isoformat()
                                })
                    except:
                        continue

            except Exception as e:
                self.logger.error(f"Table extraction failed: {e}")

            if not chains_data:
                self.logger.info("No table found, trying div extraction...")
                page_text = driver.page_source

                chain_pattern = r"(\w+).*?(\d+).*?\$([0-9,]+\.?\d*[BMK]?)"
                matches = re.findall(chain_pattern, page_text)

                for match in matches:
                    if len(match) == 3:
                        name, protocols, tvl_text = match
                        tvl = self.extract_tvl(f"${tvl_text}")

                        if include_zero_tvl or tvl > 0:
                            chains_data.append({
                                "name": name,
                                "protocols": int(protocols) if protocols.isdigit() else 0,
                                "tvl": tvl,
                                "timestamp": datetime.now().isoformat()
                            })

            return chains_data

        finally:
            driver.quit()


    def extract_number(self, text):
        if not text:
            return 0
        numbers = re.findall(r"\d+", text.replace(",", ""))
        if numbers:
            try:
                return int(numbers[0])
            except:
                return 0
        return 0

    def extract_tvl(self, text):
        if not text:
            return 0

        clean_text = re.sub(r"[$,]", "", text).strip()

        multiplier = 1
        if clean_text.endswith("B") or clean_text.endswith("b"):
            multiplier = 1_000_000_000
            clean_text = clean_text[:-1]
        elif clean_text.endswith("M") or clean_text.endswith("m"):
            multiplier = 1_000_000
            clean_text = clean_text[:-1]
        elif clean_text.endswith("K") or clean_text.endswith("k"):
            multiplier = 1_000
            clean_text = clean_text[:-1]

        try:
            return float(clean_text) * multiplier
        except:
            return 0

    def save_to_csv(self, chains_data, filename=None):
        if not chains_data:
            self.logger.warning("No data to save")
            return False

        if filename is None:
            filename = self.config["output_filename"]

        try:
            chains_data.sort(key=lambda x: (x["tvl"] == 0, -x["tvl"]))

            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = ["name", "protocols", "tvl", "timestamp"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()

                for row in chains_data:
                    writer.writerow(row)

            self.logger.info(f"Data successfully saved to {filename}")
            self.logger.info(f"Total chains: {len(chains_data)}")

            non_zero_count = sum(1 for chain in chains_data if chain["tvl"] > 0)
            zero_count = len(chains_data) - non_zero_count

            self.logger.info(f"Chains with TVL > 0: {non_zero_count}")
            self.logger.info(f"Chains with TVL = 0: {zero_count}")

            non_zero_chains = [chain for chain in chains_data if chain["tvl"] > 0]
            self.logger.info("Top 15 chains by TVL:")
            self.logger.info("-" * 70)
            for i, chain in enumerate(non_zero_chains[:15], 1):
                self.logger.info(
                    f"{i:2d}. {chain['name']:<20} | Protocols: {chain['protocols']:>4} | TVL: ${chain['tvl']:>15,.2f}")

            zero_tvl_chains = [chain for chain in chains_data if chain["tvl"] == 0]
            if zero_tvl_chains:
                self.logger.info(f"\nSample of chains with zero TVL (showing first 10 of {len(zero_tvl_chains)}):")
                self.logger.info("-" * 70)
                for i, chain in enumerate(zero_tvl_chains[:10], 1):
                    self.logger.info(
                        f"{i:2d}. {chain['name']:<20} | Protocols: "
                        f"{chain['protocols']:>4} | TVL: ${chain['tvl']:>15,.2f}")

            return True

        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            return False

    def save_historical_data(self, chains_data):
        if not self.config.get("save_historical_data", False):
            return

        try:
            hist_dir = Path(self.config["historical_data_dir"])
            hist_dir.mkdir(exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            hist_filename = hist_dir / f"defillama_chains_{timestamp}.csv"

            self.save_to_csv(chains_data, str(hist_filename))
            self.logger.info(f"Historical data saved: {hist_filename}")

        except Exception as e:
            self.logger.error(f"Error saving historical data: {e}")

    def scrape_data_with_retry(self):
        max_retries = self.config.get("max_retries", 3)
        retry_delay = self.config.get("retry_delay_seconds", 10)

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Scraping attempt {attempt + 1}/{max_retries}")

                chains_data = self.get_chains_data_api()

                if not chains_data:
                    self.logger.info("API method failed, trying Selenium...")
                    chains_data = self.get_chains_data_selenium()

                if chains_data:
                    success = self.save_to_csv(chains_data)
                    if success:
                        self.save_historical_data(chains_data)
                        self.logger.info("Scraping completed successfully")
                        return True
                    else:
                        self.logger.error("Failed to save data")

                if attempt < max_retries - 1:
                    self.logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)

            except Exception as e:
                self.logger.error(f"Scraping attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)

        self.logger.error("All scraping attempts failed")
        return False

    def run_once(self):
        self.logger.info("Starting data scraping...")
        return self.scrape_data_with_retry()

    def start_scheduler(self):
        interval = self.config.get("scrape_interval_minutes", 5)

        self.logger.info(f"Starting scheduler with {interval} minute intervals")
        self.logger.info("Press Ctrl+C to stop the scheduler")

        schedule.every(interval).minutes.do(self.run_once)

        self.run_once()

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Scheduler stopped by user")


def main():
    print("DeFiLlama Automated Chains Data Scraper")
    print("=" * 50)

    scraper = DeFiLlamaScraper()

    print(f"Configuration loaded:")
    print(f"- Scrape interval: {scraper.config['scrape_interval_minutes']} minutes")
    print(f"- Output file: {scraper.config['output_filename']}")
    print(f"- Log file: {scraper.config['log_filename']}")
    print(f"- Historical data: {'Enabled' if scraper.config['save_historical_data'] else 'Disabled'}")
    print(f"- Include zero TVL chains: {'Enabled' if scraper.config['include_zero_tvl'] else 'Disabled'}")

    proxy_config = scraper.config.get("proxy", {})
    if proxy_config.get("enabled", False):
        print(f"- Proxy: Enabled ({proxy_config.get('type', 'http')})")
        if proxy_config.get("rotate_proxies", False):
            proxy_count = len(proxy_config.get("proxy_list", []))
            print(f"- Proxy rotation: Enabled ({proxy_count} proxies)")
        else:
            print(f"- Proxy rotation: Disabled")
    else:
        print(f"- Proxy: Disabled")
    print()

    choice = input(
        "Choose an option:\n1. Run once\n2. Start scheduler\n3. Exit\nEnter choice (1-3): "
    ).strip()

    if choice == "1":
        scraper.run_once()
    elif choice == "2":
        scraper.start_scheduler()
    elif choice == "3":
        print("Exiting...")
    else:
        print("Invalid choice. Starting scheduler by default...")
        scraper.start_scheduler()


if __name__ == "__main__":
    main()
