# BargainFinder

BargainFinder identifies undervalued stocks based on recent price movements and generates automated weekly reports. It checks for significant drops over one-month and three-month windows, tracks historical bargains, and provides direct links for quick research. The program runs quietly in the background, evaluates tickers at a set hour each weekday, and sends tabular reports via email every Thursday.

---

## Table of Contents

- [Introduction](#introduction)  
- [Features](#features)  
- [Getting Started](#getting-started)  
- [Configuration](#configuration)  
- [Running in the Background](#running-in-the-background)  
- [Email Reports](#email-reports)  
- [Logging](#logging)  
- [Contributors](#contributors)  
- [License](#license)  

---

## Introduction

Markets swing. Most investors miss the right moment to capitalize. BargainFinder watches the S&P 500 daily and notifies you when stocks go on sale, such Boeing after a door blows off or Crowdstrike when they crash every corporate Windows computer. It filters tickers using percentage-based thresholds, compiles cleanly formatted summaries, and integrates them into persistent historical logs. Weekly email reports consolidate key findings, ranking frequent bargain appearances with average performance metrics.

---

## Features

- Filters stocks that fall below a defined threshold
- Logs daily bargains into a persistent history
- Sends clean email reports every Thursday
- Includes research links for each ticker
- Automatically schedules and runs silently

---

## Getting Started

1. Clone the repository  
   ```bash
   git clone https://github.com/yourname/investopps.git
   cd investopps

2. Create and activate a virtual environment  
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   source venv/bin/activate  # macOS/Linux
   ```

3. Install the dependencies  
   ```bash
   pip install -r requirements.txt
   ```

4. Set up Gmail API credentials  
   - Enable the Gmail API in your Google Cloud project  
   - Download the OAuth 2.0 credentials and save as `credentials.json`  
   - Run the script once to authorize access  

5. Prepare the ticker list  
   - Place your tickers in `meta/tickers.txt`, one per line

---

## Configuration

File structure:

```
data/
├── prices.csv
└── bargain_history.csv
meta/
├── tickers.txt
├── credentials.json
├── config.yaml
└── update_log.txt
investopps/
├── query.py
├── send_email.py
└── requirements.txt
```

---

## Running in the Background

To launch BargainFinder without a terminal:

```bash
pythonw query.py
```

To confirm it runs:
- Check Task Manager for `pythonw.exe`
- Open `log.txt` or `heartbeat.txt` for live activity
- Review `bargain_history.csv` for daily logs

To stop the script:
- Open Task Manager → End the `pythonw.exe` process
- Or use:
  ```bash
  taskkill /IM pythonw.exe /F
  ```

---

## Email Reports

The script sends weekly email reports on Thursdays at the configured hour. Each report includes:

- Tickers with repeated bargain appearances
- Average changes over 1 and 3 months
- Google Finance and Brave search links
- Clean HTML table layout

To test Gmail API access separately, run:

```python
from send_email import get_credentials
get_credentials()
```

Ensure `credentials.json` is correct and the Gmail API is enabled for your account.


---

## Contributors

Built and maintained by Dallin Stewart.  
Contact: [dallinpstewar@gmail.com]  

---

## License

This project is licensed for personal and non-commercial use.  
Reach out for custom licensing, enterprise use, or integrations.

