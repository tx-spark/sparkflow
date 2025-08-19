import pandas as pd
import requests

YEAR = str(pd.Timestamp.now().year)


def get_texas_tribune_electeds_info():
    texas_tribune_base_url = f"https://republic-api.texastribune.org/{YEAR}/"
    subdivisions = [
        "state.json",
        "texas-house.json",
        "texas-senate.json",
        "us-house.json",
    ]

    raw_dfs = []
    for subdivision in subdivisions:
        api_url = f"{texas_tribune_base_url}{subdivision}"
        raw_data = requests.get(api_url).json()

        if "results" not in raw_data.keys():
            raise ConnectionError(
                "Did not find expected data format on Texas Tribune API. Aborting."
            )

        raw_df = pd.DataFrame(raw_data["results"])
        raw_dfs.append(raw_df)

    return pd.concat(raw_dfs)


def clean_texas_tribune_electeds_info(raw_df):

    required_columns = [
        # basic personal info
        "id",
        "name",
        "birth_date",
        "hometown",
        "occupation",
        "salary",
        # Office Info
        "body",
        "division",
        "office",
        "party",
        # Addresses
        "capitol_address",
        "capitol_office",
        "district_address",
        "next_election_year",
        # Socials
        "twitter",
        "website_official",
        "website_personal",
        "facebook",
        # Contact Info
        "capitol_email",
        "capitol_phone",
        "district_phone",
    ]

    # Return empty dataframe if raw_df is None or empty
    if raw_df is None or len(raw_df) == 0:
        return pd.DataFrame(columns=required_columns)

    missing_columns = []
    for col in required_columns:
        if col not in raw_df.columns:
            missing_columns.append(col)

    if len(missing_columns) > 0:
        missing_columns_string = ", ".join(missing_columns)
        print(missing_columns_string)
        raise Exception(
            f"The Data from Texas Tribune is missing the columns: {missing_columns_string}"
        )
    clean_df = raw_df[required_columns]
    return clean_df


if __name__ == "__main__":
    raw_df = get_texas_tribune_electeds_info()
    clean_df = clean_texas_tribune_electeds_info(raw_df)
    clean_df.to_csv("data/General Info/texas_tribune_electeds_info.csv", index=False)
