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
FAKE_AGG_FILE = "pipeline_example_fake_payments.js"
MASK_AGG_FILE = "pipeline_example_mask_payments.js"


##
# Get JavaScript pipeline and transform to content that will work in Python
##
def getPipelineFromPyAggFile(dataFilename):
    importsFilename = "../lib/masksFakesGeneraters_py_imports.py"
    with open(dataFilename, mode="r") as dataFile, open(importsFilename, mode="r") as importsFile:
        pythonContent = dataFile.read()
        pythonContent = re.sub("/\\*.*?\\*/", "", pythonContent, flags=re.DOTALL)  # remove blkcmts
        pythonContent = re.sub(r"//.*?\n", "\n", pythonContent, flags=re.M)  # remove other js cmnts
        pythonContent = re.sub(r"true", "True", pythonContent, flags=re.M)  # convert to py bool
        pythonContent = re.sub(r"false", "False", pythonContent, flags=re.M)  # convert py bool
        pythonContent = re.sub(r"null", "None", pythonContent, flags=re.M)  # convert js null to py
        pipelineCode = importsFile.read() + "\nglobal pipeline;\n" + pythonContent
        # pprint(pipelineCode)
        pipelineCompiledCode = compile(pipelineCode, "pipeline", "exec")
        exec(pipelineCompiledCode)
        return pipeline


db = MongoClient(URL)[DB]
coll = db[COLL]

if DO_FAKE_RATHER_THAN_MASK and (coll.count_documents({}) <= 0):
    coll.drop()
    coll.insert_one({})
    coll.insert_one({})

pipeline = getPipelineFromPyAggFile(FAKE_AGG_FILE if DO_FAKE_RATHER_THAN_MASK else MASK_AGG_FILE)
# pprint(pipeline)

for record in coll.aggregate(pipeline):
    pprint(record)
