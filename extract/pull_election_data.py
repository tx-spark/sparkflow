import yaml
import requests
import pandas as pd
################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = 'config.yaml'


################################################################################
# MAIN
################################################################################

if __name__ == "__main__":
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    print(config['sources']['election_results']['districts'])

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(config['sources']['election_results']['districts'], headers=headers, timeout=10)

    response_data = response.json()
    df = pd.DataFrame(map(lambda a: a['Races'], response_data))

    print(df)


