import json
import os
import re
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

HOUSE_URL = "https://www.house.texas.gov/members"
SENATE_URL = "https://senate.texas.gov/members.php"


def get_senate_pages():
    senate_members_page = requests.get(SENATE_URL).text
    senate_soup = BeautifulSoup(senate_members_page, features="lxml")
    senate_memlist_div = senate_soup.find("div", class_="memlist")

    # Create directories if they don't exist
    os.makedirs("./data/temp_data/Contact Info/Official Images", exist_ok=True)

    senator_pages = []

    # Find all member divs
    member_divs = senate_memlist_div.find_all("div", class_="mempicdiv")

    for member in member_divs:

        # Get the member page URL
        member_link = member.find("a")["href"]
        member_url = urljoin(SENATE_URL, member_link)

        # Get the senator's name from the alt text of the image
        img = member.find("img")
        name = img["alt"].strip()

        # Get and save the image
        img_url = urljoin(SENATE_URL, img["src"])
        img_response = requests.get(img_url)

        if img_response.status_code == 200:
            img_path = f"./data/temp_data/Contact Info/Official Images/{'SD-' + member_url.split('/')[-1].replace('.php','').replace('member?d=','')}.jpg"
            with open(img_path, "wb") as f:
                f.write(img_response.content)

        senator_pages.append({"name": name, "page_url": member_url})

    return senator_pages


def read_senator_info_page(page_url):
    member_page_html = requests.get(page_url).text
    member_page_soup = BeautifulSoup(member_page_html, features="lxml")
    member_page_main_content = member_page_soup.find(
        "div", class_="mainbody", id="main-content"
    )
    # member_page_office_info = member_page_main_content.find_all("div",class_='onecolumn')[1]
    member_page_office_info = member_page_main_content.find(
        "div", id="mem_addrs", class_="ui-corner-all"
    )
    member_page_office_info_table = member_page_office_info.find("table")
    member_page_office_info_table_row = member_page_office_info_table.find_all("td")
    addresses = []

    # Extract district office info
    for td in member_page_office_info_table_row:
        if not td.text.strip():
            continue

        office_text = td.get_text(strip=True, separator="\n").split("\n")

        phone = None
        addr_lines = []
        fax = None

        for i in range(len(office_text)):
            line = office_text[i]
            line = line.strip()
            if line == "(TEL)":
                continue
            elif line == "(FAX)":
                continue
            elif line == "(TOLL-FREE)":
                continue
            if (
                "(TEL)" in line
                or "(TEL)" in office_text[min(i + 1, len(office_text) - 1)]
            ):
                phone = line.replace("(TEL)", "").strip()
            elif (
                "(FAX)" in line
                or "(FAX)" in office_text[min(i + 1, len(office_text) - 1)]
            ):
                fax = line.replace("(FAX)", "").strip()
            elif (
                "(TOLL-FREE)" in line
                or "(TOLL-FREE)" in office_text[min(i + 1, len(office_text) - 1)]
            ):
                fax = line.replace("(TOLL-FREE)", "").strip()
            elif line:
                addr_lines.append(line)

        district_office = {
            "type": "District",
            "address_lines": "\n".join(addr_lines),
            "phone": phone,
            "fax": fax,
        }
        addresses.append(district_office)

        # set the first one to the capitol office
        addresses[0]["type"] = "Capitol"

    return pd.DataFrame(addresses)


def get_senator_offices():
    senator_address_dfs = []
    senate_pages = get_senate_pages()
    for page in senate_pages:
        senator_name = page["name"]
        senator_url = page["page_url"]
        district = "SD-" + page["page_url"].split("/")[-1].replace(".php", "").replace(
            "member?d=", ""
        )
        senator_df = read_senator_info_page(senator_url)
        senator_df["Name"] = senator_name
        senator_df["District"] = district
        senator_address_dfs.append(senator_df)

    return pd.concat(senator_address_dfs)


def get_house_pages(get_images=False):
    house_members_page = requests.get(HOUSE_URL).text
    house_soup = BeautifulSoup(house_members_page, features="lxml")
    house_members_page_main = house_soup.find("main", id="main-content")
    # The member data is embedded in a Vue component attribute as JSON
    members_component = house_members_page_main.find("get-members")
    if not members_component:
        return []

    # Extract the members JSON from the :members attribute
    members_json_str = members_component.get(":members", "[]")
    # Clean up the string - remove escaped quotes and HTML entities
    members_json_str = members_json_str.replace("&amp;", "&")

    members = json.loads(members_json_str)

    if get_images:
        for member in members:
            # Get and save the image
            img_url = urljoin(HOUSE_URL, member["image"])

            img_response = requests.get(img_url)

            if img_response.status_code == 200:
                img_path = f"./data/temp_data/Contact Info/Official Images/{'HD-' + str(member['id'])}.jpg"
                with open(img_path, "wb") as f:
                    f.write(img_response.content)

    return members


def read_house_member_info_page(page_url):
    house_member_page = requests.get(page_url).text
    house_member_soup = BeautifulSoup(house_member_page, features="lxml")
    house_member_grid_container = house_member_soup.find(
        "section", class_="grid-container"
    )
    house_member_grid_container = house_member_grid_container.find("div")
    for div in house_member_grid_container.find_all("div"):
        div_text = div.getText()
        if "Capitol Address" in div_text or "Disctict Address" in div_text:
            type = None
            phone = None
            address_lines = ""
            addresses = []
            for line in div_text.split("\n"):
                if "Capitol Address" in line:
                    if address_lines != "":
                        addresses.append(
                            {
                                "type": type,
                                "address_lines": address_lines.strip(),
                                "phone": phone,
                            }
                        )
                        address_lines = ""
                        phone = None
                    type = "Capitol"
                    address_lines = ""
                    continue
                if "District Address" in line:
                    if address_lines != "":
                        addresses.append(
                            {
                                "type": type,
                                "address_lines": address_lines.strip(),
                                "phone": phone,
                            }
                        )
                        address_lines = ""
                        phone = None
                    type = "District"
                    continue
                if line.strip() == "":
                    continue
                # Check if line matches phone number pattern (XXX) XXX-XXXX
                if re.match(r"\(\d{3}\)\s\d{3}-\d{4}", line.strip()):
                    phone = line.strip()
                    continue
                address_lines += line + "\n"
            if address_lines != "":
                addresses.append(
                    {
                        "type": type,
                        "address_lines": address_lines.strip(),
                        "phone": phone,
                    }
                )
                address_lines = ""
                phone = None
            return pd.DataFrame(addresses)


def get_house_offices():
    house_member_address_dfs = []
    house_pages = get_house_pages()
    for page in house_pages:
        house_member_name = page["member_name"]
        house_url = page["link"]
        district = "HD-" + str(page["id"])
        house_member_df = read_house_member_info_page(house_url)
        house_member_df["Name"] = house_member_name
        house_member_df["District"] = district
        house_member_address_dfs.append(house_member_df)

    return pd.concat(house_member_address_dfs)


if __name__ == "__main__":
    senate_addresses_df = get_senator_offices()
    senate_addresses_df.to_csv(
        "data/Contact Info/senate_official_contact_info.csv", sep="\t", index=False
    )
    house_member_addresses_df = get_house_offices()
    house_member_addresses_df.to_csv(
        "data/Contact Info/house_official_contact_info.csv", sep="\t", index=False
    )

    all_member_addresses_df = pd.concat(
        [senate_addresses_df, house_member_addresses_df]
    )
    all_member_addresses_df.to_csv(
        "data/Contact Info/all_member_official_contact_info.csv", sep="\t", index=False
    )
