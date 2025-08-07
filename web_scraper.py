import time
import schedule

from config import ConfigManager
from proxy_manager import ProxyManager
from data_fetcher import DataFetcher
from data_saver import DataSaver


class DeFiLlamaScraper:
    def __init__(self, config_file="config.json"):
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.config
        self.logger = self.config_manager.setup_logging()

        self.proxy_manager = ProxyManager(self.config)
        self.data_fetcher = DataFetcher(self.config, self.proxy_manager)
        self.data_saver = DataSaver(self.config)

        if self.logger:
            self.logger.info("DeFiLlama Scraper initialized")

    def scrape_data_with_retry(self):
        max_retries = self.config.get("max_retries", 3)
        retry_delay = self.config.get("retry_delay_seconds", 10)

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Scraping attempt {attempt + 1}/{max_retries}")

                chains_data = self.data_fetcher.get_chains_data_api()

                if not chains_data:
                    self.logger.info("API method failed, trying Selenium...")
                    chains_data = self.data_fetcher.get_chains_data_selenium()

                if chains_data:
                    success = self.data_saver.save_to_csv(chains_data)
                    if success:
                        self.data_saver.save_historical_data(chains_data)
                        self.logger.info("Scraping completed successfully")
                        return chains_data
                    else:
                        self.logger.error("Failed to save data")

                if attempt < max_retries - 1:
                    self.logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    self.proxy_manager.rotate_proxy()

            except Exception as e:
                self.logger.error(f"Scraping attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    self.proxy_manager.rotate_proxy()

        self.logger.error("All scraping attempts failed")
        return None

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

    def export_data(self, chains_data=None, format_type="csv"):
        if chains_data is None:
            chains_data = self.run_once()

        if not chains_data:
            self.logger.error("No data to export")
            return False

        if format_type.lower() == "json":
            return self.data_saver.export_json(chains_data)
        elif format_type.lower() == "xlsx":
            return self.data_saver.export_xlsx(chains_data)
        elif format_type.lower() == "csv":
            return self.data_saver.save_to_csv(chains_data)
        else:
            self.logger.error(f"Unsupported export format: {format_type}")
            return False

    def get_config_summary(self):
        return {
            "scrape_interval": f"{self.config['scrape_interval_minutes']} minutes",
            "output_file": self.config['output_filename'],
            "log_file": self.config['log_filename'],
            "historical_data":
                "Enabled" if self.config['save_historical_data'] else "Disabled",
            "include_zero_tvl":
                "Enabled" if self.config['include_zero_tvl'] else "Disabled",
            "proxy_enabled":
                "Enabled" if self.config.get("proxy", {})
                .get("enabled", False) else "Disabled",
            "proxy_rotation":
                "Enabled" if self.config.get("proxy", {})
                .get("rotate_proxies", False) else "Disabled"
        }
