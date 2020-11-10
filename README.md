# spark-examples - Python 3.7.7
Repo for code spark code snippents


avroschematostruct.py
---------------------
converts the avroschema to SQL struct

hiverunex.py
------------
test code from https://pypi.org/project/py-hiverunner/
used to mimic hive interface.

1. python -m pip install py-hiverunner
2. docker pull la9ran9e/py-hiverunner
3. docker run -ti -p 25333:25333 -p 25334:25334 la9ran9e/py-hiverunner
4. Note: In case you get MapRedTask. No space available in any of the local directories, delete the dangling docker images to free up space
docker image prune


