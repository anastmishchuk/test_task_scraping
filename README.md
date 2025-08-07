# DeFiLlama Chains Data Scraper

Automated Python scraper for collecting blockchain chain data from DeFiLlama, including TVL and protocol counts.

## Features

- API-based scraping with Selenium fallback
- Automated scheduling with configurable intervals
- Proxy support and rotation
- CSV export with historical data archival
- Comprehensive logging and error handling

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Install ChromeDriver:
```bash
# Download from https://chromedriver.chromium.org/
# Or use package manager:
brew install chromedriver  # macOS
choco install chromedriver # Windows
```

## Quick Start

```bash
python scraper.py
```

Choose option:
- `1` - Run once
- `2` - Start scheduler  
- `3` - Exit

## Configuration

Edit `config.json` (auto-created on first run):

```json
{
    "scrape_interval_minutes": 5,
    "output_filename": "defillama_chains.csv",
    "save_historical_data": false,
    "include_zero_tvl": true,
    "proxy": {
        "enabled": false,
        "host": "proxy.example.com",
        "port": 8080,
        "rotate_proxies": false
    }
}
```

## Output

CSV format with columns: `name`, `protocols`, `tvl`, `timestamp`

```csv
name,protocols,tvl,timestamp
Ethereum,850,45678901234.56,2024-01-15T10:30:00
BSC,425,12345678901.23,2024-01-15T10:30:00
```
