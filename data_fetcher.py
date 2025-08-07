import time
import re
import logging
import urllib.parse
from collections import defaultdict
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


class DataFetcher:
    def __init__(self, config, proxy_manager):
        self.config = config
        self.proxy_manager = proxy_manager
        self.logger = logging.getLogger(__name__)

    def get_chains_data_api(self):
        try:
            self.logger.info("Fetching chains data from DeFiLlama API...")

            chains_url = "https://api.llama.fi/v2/chains"
            session = self.proxy_manager.get_session()
            response = session.get(chains_url, timeout=30)
            response.raise_for_status()
            chains_data = response.json()

            self.logger.info(f"Found {len(chains_data)} chains")

            self.logger.info("Fetching protocols data to count protocols per chain...")
            protocols_url = "https://api.llama.fi/protocols"
            response = session.get(protocols_url, timeout=30)
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

            self.logger.info(
                f"Filtered {len(csv_data)} chains (include_zero_tvl: {include_zero_tvl})"
            )

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
        chrome_options.add_experimental_option(
            "excludeSwitches", ["enable-automation"]
        )
        chrome_options.add_experimental_option(
            "useAutomationExtension", False
        )

        proxy_url = self.proxy_manager.get_proxy_for_selenium()
        if proxy_url:
            parsed = urllib.parse.urlparse(proxy_url)
            if parsed.hostname and parsed.port:
                chrome_options.add_argument(f"--proxy-server={parsed.hostname}:{parsed.port}")
                self.logger.info(f"Selenium using proxy: {parsed.hostname}:{parsed.port}")

                if parsed.username and parsed.password:
                    self.logger.warning(
                        "Selenium proxy authentication may require additional setup"
                    )

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

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
                                protocols = self._extract_number(protocols_text)
                                tvl = self._extract_tvl(tvl_text) if "$" in tvl_text else 0

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
                        tvl = self._extract_tvl(f"${tvl_text}")

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

    def _extract_number(self, text):
        if not text:
            return 0
        numbers = re.findall(r"\d+", text.replace(",", ""))
        if numbers:
            try:
                return int(numbers[0])
            except:
                return 0
        return 0

    def _extract_tvl(self, text):
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
