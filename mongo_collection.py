from os import getenv
from urllib import parse
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_USERNAME = getenv("MONGO_USERNAME")
MONOGO_PASSWORD = getenv("MONOGO_PASSWORD")
MONGO_HOST = getenv("MONGO_HOST")
MONGO_PORT = getenv("MONGO_PORT")
MONDODB = getenv("MONDODB")


def get_mongo_coll():
    """get_mongo_collection function returns report, uns, reportemail, reportalert collections"""
    username = parse.quote_plus(MONGO_USERNAME)
    password = parse.quote_plus(MONOGO_PASSWORD)

    CONNECTION_STRING = f"mongodb://{username}:{password}@{MONGO_HOST}:{MONGO_PORT}/{MONDODB}?authMechanism=SCRAM-SHA-1"
    mongo_database = MongoClient(CONNECTION_STRING).get_database("sigview2")
    report_coll = mongo_database.get_collection("report")
    uns_coll = mongo_database.get_collection("users_new_sample")
    reportemail_coll = mongo_database.get_collection("reportemail")
    reportalert_coll = mongo_database.get_collection("reportalert")

    return report_coll, uns_coll, reportemail_coll, reportalert_coll
