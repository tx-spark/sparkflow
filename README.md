# texas-leg-scraper

This is a scraper for the Texas Legislature website.

## Setup

### Poetry
I used poetry for this project to maintain dependencies. First install poetry, then run `poetry install` to install the dependencies.

### Gspread
This project uses gspread to write to google sheets. You'll need to set up a service account, and set it up with gspread to use this project. Gspread has a [quickstart guide](https://docs.gspread.org/en/v6.1.3/oauth2.html#enable-api-access-for-a-project) that will walk you through the setup process.

### Scraper config
Go over all the configuration options setup in `scraper/scraper_config.yaml`, and make sure they're correct for your use case.

## Running the scraper

Run the scraper with `poetry run python scraper/scraper.py`.


