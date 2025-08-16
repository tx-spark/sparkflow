import duckdb
import pandas as pd
import requests

voting_rights_lab_url = "https://tracker.votingrightslab.org/storage/bills-2025.json"

result = requests.get(voting_rights_lab_url)

result_json = result.json()
result_json_data = result_json["data"]
for item in result_json_data:
    tags = item["tags"]
    for key in tags.keys():
        item[f"tags{key}"] = (
            tags[key] if type(tags[key]) != list else [a["tag"] for a in tags[key]]
        )

voting_rights_lab_df = pd.DataFrame(result_json_data)

raw_texas_bills = voting_rights_lab_df[voting_rights_lab_df["state"] == "TX"]

raw_texas_bills["bill_id"] = raw_texas_bills.apply(
    lambda a: (
        f"{a['legtype']}B{str(a['bill_number'])}"
        if a["legtype"] in ("H", "S")
        else f"{a['legtype']}{str(a['bill_number'])}"
    ),
    axis=1,
)
raw_texas_bills["for_against_on"] = raw_texas_bills["tags-Impact"].apply(
    lambda a: (
        "Against"
        if a[0] == "Anti-voter"
        else "For" if a[0] == "Pro-voter" else "On" if a[0] == "Mixed_Unclear" else a[0]
    )
)  # In the future change this to use all values if they exist. Not sure why this would happen though


voting_rights_lab_tags_url = "https://tracker.votingrightslab.org/storage/tags.json"

raw_tags = requests.get(voting_rights_lab_tags_url).json()["data"]
raw_tags
clean_tags = []
for i in raw_tags:
    tag = {
        "name": i["name"],
        "label_long": i["label"],
        "group": i["group"],
        "label": i["group_label"],
    }
    clean_tags.append(tag)

for tag in clean_tags:
    if f'tags{tag["group"]}' in raw_texas_bills.columns:
        # print(tag['name'])
        # print(raw_texas_bills[f'tags{tag["group"]}'][raw_texas_bills[f'tags{tag["group"]}'].notna()])
        raw_texas_bills[f'tags{tag["group"]}'] = raw_texas_bills[
            f'tags{tag["group"]}'
        ].apply(
            lambda a: (
                a
                if type(a) != list
                else [tag["label"] if t == tag["name"] else t for t in a]
            )
        )
        # print(raw_texas_bills[f'tags{tag["group"]}'][raw_texas_bills[f'tags{tag["group"]}'].notna()])
# raw_texas_bills = raw_texas_bills.reset_index()  # make sure indexes pair with number of rows

tags_columns = list(
    filter(lambda a: a[:4] == "tags" and len(a) > 4, list(raw_texas_bills.columns))
)
print(tags_columns)

clean_texas_bills = []

for index, row in raw_texas_bills.iterrows():
    for tag_col in tags_columns:
        if type(row[tag_col]) == list:
            for item in row[tag_col]:
                clean_texas_bills.append(
                    {
                        "bill_id": row["bill_id"],
                        "for_against_on": row["for_against_on"],
                        "Organization": "Voting Rights Lab",
                        "tag": item,
                    }
                )
print(clean_texas_bills)

voting_rights_lab_bills = pd.DataFrame(clean_texas_bills)
voting_rights_lab_bills = voting_rights_lab_bills[
    ~voting_rights_lab_bills["tag"].isin(["", "Bill Impact", "Stage"])
]
voting_rights_lab_bills = duckdb.sql(
    "select bill_id,for_against_on,Organization,tag from voting_rights_lab_bills group by 1,2,3,4"
).df()
voting_rights_lab_bills = voting_rights_lab_bills[
    voting_rights_lab_bills["tag"].str.contains(" ")
]
voting_rights_lab_bills.to_csv("Voting_Rights_Lab_Tags.csv", index=False, sep="\t")
