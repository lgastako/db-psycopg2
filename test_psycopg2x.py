import os

import db

from db_psycopg2 import Psycopg2Driver as Driver


def test_from_url():
    driver = Driver.from_url("postgresql://user:pass@host/db_name")
    assert driver.conn_kwargs == {
        "user": "user",
        "host": "host",
        "password": "pass",
        "port": 5432,
        "dbname": "db_name"}

def test_operations():
    test_db = db.from_environ("DB_PSYCOPG2_TEST_URL")
    test_db.do("DROP TABLE IF EXISTS conn_test;")
    test_db.do("CREATE TABLE conn_test (name TEXT)")
    assert test_db.count("conn_test") == 0
    test_db.do("INSERT INTO conn_test (name) VALUES ('foo');")
    test_db.do("INSERT INTO conn_test (name) VALUES ('bar');")
    assert test_db.count("conn_test") == 2
    assert set([x.name for x in db.items("SELECT * FROM conn_test")]) == \
            set(["foo", "bar"])

def test_search_path():
    url = os.environ["DB_PSYCOPG2_TEST_URL"]

    test_db = db.from_url(url)
    test_db.do("DROP SCHEMA IF EXISTS other_schema CASCADE;");
    test_db.do("CREATE SCHEMA other_schema;");
    test_db.do("CREATE TABLE other_schema.path_test (name TEXT)")

    if "?" not in url:
        url += "?"
    url += "&search_path=other_schema"

    test_db2 = db.from_url(url)

    assert test_db2.count("other_schema.path_test") == 0
    test_db2.do("INSERT INTO other_schema.path_test (name) VALUES ('foo');")
    test_db2.do("INSERT INTO other_schema.path_test (name) VALUES ('bar');")
    assert test_db2.count("other_schema.path_test") == 2

    assert set([x.name for x in test_db2.items("SELECT * FROM path_test")]) == \
           set(["foo", "bar"])

def test_condense_unilists():
    assert Driver.condense_unilists({
        "a": "b",
        "c": ["d"],
        "e": ["f", "g"],
        "h": 4
    }) == {
        "a": "b",
        "c": "d",
        "e": ["f", "g"],
        "h": 4
    }

def test_get_kwargs():
    assert Driver._get_kwargs("postgresql://host/dbname?a=b&c=def") == {
        "a": "b",
        "c": "def"
    }
    assert Driver._get_kwargs("postgresql://host/dbname") == {}
