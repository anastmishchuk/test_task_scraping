import csv
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd


class DataSaver:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

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

            self._log_data_summary(chains_data)
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

    def _log_data_summary(self, chains_data):
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

    def export_json(self, chains_data, filename=None):
        if not chains_data:
            self.logger.warning("No data to export to JSON")
            return False

        if filename is None:
            filename = self.config["output_filename"].replace(".csv", ".json")

        try:
            with open(filename, "w", encoding="utf-8") as jsonfile:
                json.dump(chains_data, jsonfile, indent=2, ensure_ascii=False)

            self.logger.info(f"Data successfully exported to JSON: {filename}")
            return True

        except Exception as e:
            self.logger.error(f"Error exporting to JSON: {e}")
            return False

    def export_xlsx(self, chains_data, filename=None):
        if not chains_data:
            self.logger.warning("No data to export to Excel")
            return False

        if filename is None:
            filename = self.config["output_filename"].replace(".csv", ".xlsx")

        try:
            df = pd.DataFrame(chains_data)
            df.to_excel(filename, index=False, engine='openpyxl')

            self.logger.info(f"Data successfully exported to Excel: {filename}")
            return True

        except Exception as e:
            self.logger.error(f"Error exporting to Excel: {e}")
            return False
