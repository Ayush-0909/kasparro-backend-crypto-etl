\# 🚀 Kasparro – Crypto Backend \& ETL System



This project implements a production-style backend and ETL pipeline for ingesting,

normalizing, and serving cryptocurrency market data.



It is built to satisfy \*\*P0 + P1 requirements\*\* of the Kasparro Backend \& ETL Systems assignment.



---



\## 🧱 Architecture Overview



\- \*\*ETL Layer\*\*

&nbsp; - CSV ingestion

&nbsp; - CoinPaprika API ingestion

&nbsp; - CoinGecko API ingestion

&nbsp; - Incremental ingestion with checkpoints

&nbsp; - Normalized unified schema



\- \*\*Backend API\*\*

&nbsp; - FastAPI service

&nbsp; - `/data` endpoint with pagination \& filtering

&nbsp; - `/health` endpoint for DB + ETL status

&nbsp; - `/stats` endpoint for ETL run metadata



\- \*\*Database\*\*

&nbsp; - PostgreSQL (Docker)

&nbsp; - Raw tables + normalized tables

&nbsp; - ETL run tracking \& checkpoints



---



\## 📂 Project Structure





