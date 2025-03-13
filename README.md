# texas-leg-scraper

This is a scraper for the Texas Legislature website. See the tracker [here!](https://docs.google.com/spreadsheets/d/1mB-wBoFKuu4kS482Yc2FQn6kByDJi6uIgm7l09V_8PQ/edit?usp=sharing).

---
## Setup

### Poetry
This project uses poetry for dependency management. First [install poetry](https://python-poetry.org/docs/), then run `poetry install` to install the dependencies.

### Gspread
This project uses gspread to write to google sheets. You'll need to set up a service account, and set it up with gspread to use this project. Gspread has a [quickstart guide](https://docs.gspread.org/en/v6.1.3/oauth2.html#enable-api-access-for-a-project) that will walk you through the setup process.

### Scraper config
Go over all the configuration options setup in `config.yaml`, and make sure they're correct for your use case.

---

## Usage

The short description of the process:
- `poetry shell` to activate the poetry environment.
- `python extract/texas_leg_data_pipeline.py` will read in the bill data from the FTP server, and bill stage from the public website. This process takes about an hour. It then merges the new data with the currently existing data in to a database called `texas_bills`. In this database, each aspect of the bill (bill authors, bill companions, etc) is stored in a separate table. 
- `dbt run` will create a series of new tables off of the raw data, which all feed into the `bills.tracker` table. This table mirrors the structure of the spreadsheet.
- `dbt test` will check if the data is in good shape.
- `python extract/upload_to_google_sheets.py` takes this data from the database, and pastes it into the google sheet.

## Next steps
- Working on adding in some more tests to the dbt project.
- Working on more robust orchestration of the process (Currently just using cron jobs)
- Working on logging and monitoring of the pipeline process.

---
## Browsing the data
Some options:
- [duckcli](https://pypi.org/project/duckcli/)
- [DuckDB CLI](https://duckdb.org/docs/installation/?environment=cli)
- [How to set up DBeaver SQL IDE for DuckDB](https://duckdb.org/docs/guides/sql_editors/dbeaver)

---
For more information on dbt:
- Read the [introduction to dbt](https://docs.getdbt.com/docs/introduction)
- Read the [dbt viewpoint](https://docs.getdbt.com/docs/about/viewpoint)
- Join the [dbt Community](http://community.getdbt.com/)


