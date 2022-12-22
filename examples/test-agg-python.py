#!/usr/bin/python3
import re
import sys
from pprint import pprint
from pymongo import MongoClient
sys.path.insert(0, '../lib/')


# Constants
URL = "mongodb://localhost:27017"
DO_FAKE_RATHER_THAN_MASK = True
DB = "test"
COLL = "testdata"
FAKE_AGG_FILE = "pipeline_example_fake_accounts.js"
MASK_AGG_FILE = "pipeline_example_mask_accounts.js"


##
# Get JavaScript pipeline and transform to content that will work in Python
##
def get_pipeline_from_py_agg_file(data_filename):
    imports_filename = "../lib/masksFakesGeneraters_py_imports.py"
    
    with open(data_filename, mode="r") as dataFile, open(imports_filename, mode="r") as importsFile:
        python_content = dataFile.read()
        python_content = re.sub(r"/\*.*?\*/", "", python_content, flags=re.DOTALL)  # remove blkcmts
        python_content = re.sub(r"//[^\n]*", "\n", python_content, flags=re.M)  # remove line cmnts
        python_content = python_content.replace("true", "True")  # convert js to py bool
        python_content = python_content.replace(r"false", "False")  # convert js to py bool
        python_content = python_content.replace(r"null", "None")  # convert js null to py
        pipeline_code = importsFile.read() + "\nglobal pipeline;\n" + python_content
        # pprint(pipelineCode)
        pipeline_compiled_code = compile(pipeline_code, "pipeline", "exec")
        exec(pipeline_compiled_code)
        return pipeline


db = MongoClient(URL)[DB]
coll = db[COLL]

if DO_FAKE_RATHER_THAN_MASK and (coll.count_documents({}) <= 0):
    coll.drop()
    coll.insert_one({})
    coll.insert_one({})

pipeline = get_pipeline_from_py_agg_file(FAKE_AGG_FILE if DO_FAKE_RATHER_THAN_MASK
                                         else MASK_AGG_FILE)
# pprint(pipeline)

for record in coll.aggregate(pipeline):
    pprint(record)
