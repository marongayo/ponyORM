from pony.orm import *
import csv
import re
import json
from datetime import datetime
import xml.etree.ElementTree as ET


class Wrangler:
    def __init__(self):
        self.combined_data = []

    def csv_wrangler(self, csv_file_path):
        modified_list = []
        with open(csv_file_path, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                modified_row = {
                    "last_name"
                    if key.lower() == "second name"
                    else re.sub(r"[()]", "", key).lower().replace(" ", "_"): int(value)
                    if key.lower() in ["age (years)", "vehicle year"]
                    and value.isdigit()
                    else value
                    for key, value in row.items()
                }
                modified_list.append(modified_row)
        self.merge_person_data(modified_list)

    def get_combined_data(self):
        return self.combined_data

    def merge_person_data(self, json_data_list1):
        merged_data = {}

        for person_data in json_data_list1:
            key = (
                person_data["first_name"],
                person_data["last_name"],
                person_data["age_years"],
            )
            if key in merged_data:
                merged_data[key].update(person_data)
            else:
                merged_data[key] = person_data

        for person_data in self.combined_data:
            key = (
                person_data["first_name"],
                person_data["last_name"],
                person_data["age_years"],
            )
            if key in merged_data:
                merged_data[key].update(person_data)
            else:
                merged_data[key] = person_data

        self.combined_data = list(merged_data.values())

    def json_wrangler(self, json_file_path):
        wrangled_object_list = []
        with open(json_file_path, "r") as file:
            data = json.load(file)
        for json_object in data:
            new_object = self.rename_keys(json_object)
            wrangled_object_list.append(new_object)
        self.merge_person_data(wrangled_object_list)

    def xml_to_dict(self, xml_file_path):
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        users_data = []
        for user_elem in root.findall("user"):
            user_data = dict(user_elem.attrib)
            user_data = self.rename_keys(user_data)
            users_data.append(user_data)
        self.merge_person_data(users_data)

    def rename_keys(self, obj):
        new_obj = {}
        for key, value in obj.items():
            new_key = re.sub(r"([a-z])([A-Z])", r"\1_\2", key)
            new_key = re.sub(r"^(age)$", r"\1_years", new_key)

            if key == "debt":
                if isinstance(value, dict):
                    new_obj["debt_amount"] = float(value.get("amount", 0))
                    new_obj["debt_time_period_years"] = value.get(
                        "time_period_years", 0
                    )
                else:
                    new_obj["debt_amount"] = float(value) if value else 0
            elif key in ["credit_card_start_date", "credit_card_end_date"]:
                new_obj[new_key.lower()] = datetime.strptime(value, "%m/%y").strftime(
                    "%m/%d"
                )
            elif key in [
                "credit_card_number",
                "credit_card_security_code",
                "age",
                "dependants",
                "salary",
                "pension",
            ]:
                new_obj[new_key.lower()] = int(value) if value else None

            elif key == "commute_distance":
                new_obj[new_key.lower()] = float(value) if value else None

            elif key == "retired":
                new_obj[new_key.lower()] = value.lower() == "true"
            else:
                new_obj[new_key.lower()] = value
        return new_obj

    @db_session
    def insert_if_not_existing(self, person):
        if isinstance(person, dict):
            first_name = person.get("first_name")
            last_name = person.get("last_name")
            age_years = person.get("age_years")

            conditions = [
                f"{column} = {value}"
                if isinstance(value, (int, float))
                else f'{column} = "{value}"'
                for column, value in zip(
                    (
                        "first_name",
                        "last_name",
                        "age_years",
                    ),
                    (
                        first_name,
                        last_name,
                        age_years,
                    ),
                )
            ]

            where_clause = " AND ".join(conditions)
            select_query = f"SELECT * FROM person WHERE {where_clause}"
            in_db = db.select(select_query)

            if not in_db:
                Person(**person)


db = Database(
    provider="mysql",
    host="localhost",
    user="root",
    passwd="divergent",
    database="python_etl",
)


class Person(db.Entity):
    first_name = Required(str)
    last_name = Required(str)
    age_years = Required(int)
    sex = Optional(str)
    vehicle_make = Optional(str)
    vehicle_model = Optional(str)
    vehicle_year = Optional(int)
    vehicle_type = Optional(str)
    retired = Optional(bool)
    dependants = Optional(int)
    marital_status = Optional(str)
    salary = Optional(float)
    pension = Optional(int)
    company = Optional(str)
    commute_distance = Optional(float)
    address_postcode = Optional(str)
    iban = Optional(str)
    credit_card_number = Optional(int, size=64)
    credit_card_security_code = Optional(int)
    credit_card_start_date = Optional(str)
    credit_card_end_date = Optional(str)
    address_main = Optional(str)
    address_city = Optional(str)
    debt_amount = Optional(float)
    debt_time_period_years = Optional(int)


db.generate_mapping(create_tables=True)


engine = Wrangler()
engine.csv_wrangler(csv_file_path=f"user_data_23_4.csv")
engine.json_wrangler(json_file_path="user_data_23_4.json")
engine.xml_to_dict("user_data_23_4.xml")
people = engine.get_combined_data()


for person in people:
    engine.insert_if_not_existing(person=person)
