# End-to-End Chess.com Data Engineering Project (Python ETL)

This project builds an automated Python ETL pipeline using the Chess.com public API to transform raw chess game data into a clean, enriched, analytics-ready dataset for BI.

The pipeline is modular, incremental, and scheduled, following core data engineering best practices.

---

## How to Run the Project

To use this project, update the `USERNAME` variable inside the `config.py` file with any Chess.com username you would like to analyze.

After updating the configuration, run the main pipeline by executing:

`ETL.py`

The pipeline will automatically extract the user's games from the Chess.com API, clean and transform the raw data, enrich it with additional features, and generate a structured dataset ready for analysis. Once it's done, you can open the `chess_games.pbix` file, click the refresh button and your dashboards will be ready in a few seconds.

---

## What I Built

- Extracted chess games and player data from the Chess.com API  
- Designed a modular ETL pipeline (Extract → Transform → Enrich)  
- Cleaned and normalized raw API data  
  - Flattened JSON  
  - Standardized dates, results, and time controls  
  - Handled missing and inconsistent values  
- Enriched data with derived features  
  - Opponent metadata  
  - Country information  
  - Time-based features  
- Implemented incremental processing to avoid reprocessing data  
- Automated daily runs with GitHub Actions  
- Prepared datasets for Power BI 

---

## Tech Stack

- Python, Pandas  
- Chess.com Public API  
- GitHub Actions  
- Power BI 
- ETL Architecture (Extract → Transform → Enrich)  

---

## Key Concepts

- Modular ETL design  
- Data cleaning & enrichment  
- Incremental pipelines  
- Automation & scheduling  
- Analytics-ready data modeling  
