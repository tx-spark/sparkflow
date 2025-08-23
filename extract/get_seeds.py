import os

import duckdb
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv

################################################################################
# CONFIGURATION
################################################################################

CONFIG_PATH = "config.yaml"
load_dotenv()

LEGISCAN_API_KEY = os.getenv("LEGISCAN_API_KEY")

################################################################################
# HELPER FUNCTIONS
################################################################################


def get_session_people(session_id):
    """
    Get list of legislators active in a given session from LegiScan API.
    Session ID comes in format like '89R' and needs to be converted to numeric session_id
    by first getting session list.
    """
    STATE = "TX"

    # First get session list to map session name to ID
    url = f"https://api.legiscan.com/?key={LEGISCAN_API_KEY}&op=getSessionList&state={STATE}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    data = response.json()
    if data["status"] != "OK":
        raise Exception(f"API returned error status: {data['status']}")

    # Extract legislature number and type from input (e.g. '89R' -> 89, Regular)
    leg_num = int(session_id[:-1])
    session_type = "Regular Session" if session_id[-1] == "R" else "Special Session"

    # Find matching session ID
    numeric_session_id = None
    for session in data["sessions"]:
        if f"{leg_num}th Legislature {session_type}" in session["session_name"]:
            numeric_session_id = session["session_id"]
            break

    if not numeric_session_id:
        raise Exception(f"Could not find session ID for {session_id}")

    # Now get the legislators
    url = f"https://api.legiscan.com/?key={LEGISCAN_API_KEY}&op=getSessionPeople&id={numeric_session_id}&state={STATE}"
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    data = response.json()
    if data["status"] != "OK":
        raise Exception(f"API returned error status: {data['status']}")

    legislators = []
    for person in data["sessionpeople"]["people"]:
        legislators.append(
            {
                "people_id": person["people_id"],
                "person_hash": person["person_hash"],
                "party": person["party"],
                "role": person["role"],
                "name": person["name"],
                "first_name": person["first_name"],
                "middle_name": person["middle_name"],
                "last_name": person["last_name"],
                "suffix": person["suffix"],
                "district": person["district"],
                "votesmart_id": person["votesmart_id"],
                "ballotpedia": person["ballotpedia"],
            }
        )

    return pd.DataFrame(legislators)


def get_election_results(config):
    """
    Get election results data from the Texas Secretary of State website.
    Returns a DataFrame with election results.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(
        config["sources"]["election_results"]["districts"], headers=headers, timeout=10
    )
    response_data = response.json()

    election_results_list = []
    for race in response_data["Races"]:
        for candidate in race["Candidates"]:
            election_results_list.append(
                {
                    "id": race["id"],
                    "race_name": race["N"],
                    "candidate_id": candidate["ID"],
                    "candidate_name": candidate["N"],
                    "candidate_party": candidate["P"],
                    "candidate_votes": candidate["V"],
                    "candidate_percentage": candidate["PE"],
                }
            )
    return pd.DataFrame(election_results_list)


def get_committee_membership(leg_id, committee_code):
    committee_membership_data_url = f"https://capitol.texas.gov/Committees/MembershipCmteHist.aspx?LegSess={leg_id}&CmteCode={committee_code}"

    # Get the committee membership page
    response = requests.get(committee_membership_data_url)

    # Parse with BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")

    if len(soup.find_all("table")) < 7:
        return pd.DataFrame(
            columns=[
                "leg_id",
                "committee_code",
                "position",
                "member",
                "start_date",
                "end_date",
            ]
        )
    # Print the parsed HTML
    committee_table = soup.find_all("table")[6]

    committee_membership_data = []
    for row in committee_table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) <= 2:
            continue
        committee_membership_data.append(
            {
                "leg_id": leg_id,
                "committee_code": committee_code,
                "position": cells[0]
                .text.strip()
                .replace("Chair:", "Chair")
                .replace("Vice Chair:", "Vice Chair")
                or "Member",
                "member": cells[1].text.strip(),
                "start_date": cells[2].text.strip(),
                "end_date": cells[3].text.strip(),
            }
        )

    return pd.DataFrame(committee_membership_data)


def get_all_committee_memberships(db_path):
    try:
        conn = duckdb.connect(db_path)
        try:
            committee_codes = (
                conn.sql(
                    "select committee_code from bills.curr_committee_meetings group by 1 order by 1"
                )
                .df()["committee_code"]
                .tolist()
            )
            conn.close()
        except Exception as e:
            print(f"Error querying committee codes: {e}")
            committee_codes = []
    except Exception as e:
        print(f"Error connecting to database: {e}")
        committee_codes = []

    committee_membership_df = pd.DataFrame(
        columns=[
            "leg_id",
            "committee_code",
            "position",
            "member",
            "start_date",
            "end_date",
        ]
    )
    for committee_code in committee_codes:
        try:
            membership_data = get_committee_membership("89R", committee_code)
            if not membership_data.empty:
                committee_membership_df = pd.concat(
                    [committee_membership_df, membership_data]
                )
            else:
                print(f"No membership data found for committee {committee_code}")
        except Exception as e:
            print(f"Error processing committee {committee_code}: {e}")
            continue

    return committee_membership_df


################################################################################
# MAIN
################################################################################

if __name__ == "__main__":
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    session_id = config["info"]["LegSess"]

    # Get legislators data
    legislators_df = get_session_people(session_id)

    # Get election results
    election_results_df = get_election_results(config)

    # Committees results
    committee_membership_df = get_all_committee_memberships(
        f"{config['info']['pipeline_name']}.duckdb"
    )

    # Save to seeds folder
    legislators_df.to_csv("seeds/legislators.csv", index=False)
    election_results_df.to_csv("seeds/election_results.csv", index=False)
    committee_membership_df.to_csv("seeds/committee_membership.csv", index=False)
