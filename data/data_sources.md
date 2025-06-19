# Data Sources

## Demographic Data
- **Demographics for each VTD**: [Geographic Data/VTD_TX_Demographics.csv](https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/bf9b54a8-090c-41d0-8f00-e263fc1789c5/download/vtds_24pg_pop.zip) -  Materialized as `txspark.tx_leg_raw_bills.vtd_tx_demographics`
- **VTD Shapefile**: [Geographic Data/2024_VTD_Shapefiles](https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/906f47e4-4e39-4156-b1bd-4969be0b2780/download/vtds_24pg.zip) - Converted to GeoJSON on BigQuery in the table `txspark.tx_leg_bills.tx_leg_precincts_geography`
- **VTD Election Results**: [temp_data/election_results](https://data.capitol.texas.gov/dataset/35b16aee-0bb0-4866-b1ec-859f1f044241/resource/e1cd6332-6a7a-4c78-ad2a-852268f6c7a2/download/2024-general-vtds-election-data.zip) - All general election results are cleaned and combined at `lgover.tx_leg_bills.historical_precinct_election_results`