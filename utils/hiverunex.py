# https://pypi.org/project/py-hiverunner/
from py_hiverunner import hiverunner
from pprint import pprint


with hiverunner() as hive:
    hive.execute_query("create schema empdb")
    hive.execute_query("CREATE TABLE IF NOT EXISTS empdb.employee ( eid int, name String, salary String, dept String) COMMENT 'Employee details' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE")
    hive.execute_query("insert into empdb.employee select 1, 'test1', '1000', 'DEV'")


    print("RESULT:")
    pprint(hive.execute_query("""
        select
            *
        from
            empdb.employee
    """))