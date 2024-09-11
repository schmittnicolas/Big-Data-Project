# Dash Application in Python for Visualizing Stock Market Data
![bollinger](https://github.com/user-attachments/assets/c753ac72-5779-4a94-9f21-9bc9626bfae7)
The needed data can be found here https://www.lrde.epita.fr/~ricou/pybd/projet/boursorama.tar

## Prerequisites

- 32 GB of available storage

- Sufficient RAM (The project requires a significant amount of RAM. If possible, please open only terminal windows for containers and avoid running other processes like Firefox or other applications.)
- Navigate to `/srv/libvirt-workdir` on the school machines and clone the project.

## Launching the Application

- Navigate to the `docker` directory.
1. Start the database service:
   
   ```sh
   docker compose up db
   ```
   Wait for the database service to be ready.

2. Start the analyzer service:
   
   ```sh
   docker compose up analyzer
   ```
   This process takes about 50 minutes on the school machines.

3. Once the analysis is complete, start the dashboard:

   ```sh
   docker compose up dashboard
   ```

## Analyzer

### Market 
After extensive research on the Boursorama website, we identified patterns for each market:
 - Paris: Symbols that start with "1rP" or "1rEP"
 - Brussels: Symbols that start with "FF1"
 - Amsterdam: Symbols that start with "1rA"
 - Nasdaq: All other symbols that do not match the aforementioned prefixes

Note that this is based on our personal observations. We couldn't find any established patterns, particularly for the NASDAQ market, which accounts for about 70% of the data and is found in files starting with "amsterdam*".

### Data Cleaning

We chose to remove all records with volumes equal to 0, as it seemed unusual to have zero volumes in the middle of the trading day.

### Companies

We retained all companies, including those that appeared only once over the past five years.

### Insertion

The data insertion process takes approximately 50 minutes. We would like to mention that we developed an extremely efficient multi-processing version, but it's impossible to run it on the EPITA PCs.

### Database
![TimeScaleDB drawio1](https://github.com/user-attachments/assets/dbe779a1-99d2-4587-b039-a4bb7e9834dc)


## Dashboard

### Stock Selection (Top-Left Panel)

- Market selection (multiple choices allowed; use the cross icon to remove a choice by scrolling inside the dropdown)
- Stock selection (multiple choices allowed; use the cross icon to remove a choice by scrolling inside the dropdown)
- Stock selection for Bollinger Bands

### Date Selection (Right Panel)

- Start date and end date
- Additional feature: End date and slider to choose the interval (1D, 1W, 1M, 3M, 6M, 1Y, 5Y, 10Y)

### Graph Selection (Left Panel)

- 3 selectable graphs with tabs

### Summary Table (Right Panel)

- Display of summary data
