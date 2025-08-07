import json
import os
import logging


class ConfigManager:
    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.config = self.load_config()

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

        return logging.getLogger(__name__)
