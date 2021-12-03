#!/usr/bin/python3
##
# Lightweight MongoDB Python utility used to initiate a high throughput data ingestion or
# transformation workload against a remote MongoDB database (whether self-managed or in MongoDB
# Atlas).
#
# Ensure this '.py' script has executable permissions in your OS.
#
# For a full description of the tool and how to invoke it with various options, run:
#  $ ./mongo-mangler.py -h
#
# Example:
#  $ ./mongo-mangler --url 'mongodb+srv://usr:pwd@mycluster.abc.mongodb.net/' -s 1000000
#
# Prerequisites: Python 3.8+ and the PyMongo driver - example to install:
#  $ pip3 install --user pymongo
##
import sys
import os
import argparse
import math
import time
import re
from os import access, R_OK
from os.path import isfile
from datetime import datetime
from collections import namedtuple
from pprint import pprint
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from bson.max_key import MaxKey
from multiprocessing import Process
sys.path.insert(0, './lib/')


# Named tuple to capture info on scope for a batch aggregation run
AggBatchMetadata = namedtuple("AggBatchMetadata", ["limit", "skip"])


##
# Main function to parse the passed-in parameters before invoking the core processing function.
##
def main():
    argparser = argparse.ArgumentParser(description="Lightweight MongoDB Python utility used to "
                                                    "initiate a high throughput data ingestion or "
                                                    "transformation workload against a remote "
                                                    "MongoDB database (whether self-managed or in "
                                                    "MongoDB Atlas).")
    argparser.add_argument("-m", "--url", default=DEFAULT_MONGODB_URL,
                           help=f"MongoDB cluster URL (default: {DEFAULT_MONGODB_URL})")
    argparser.add_argument("-d", "--dbsrc", default=DEFAULT_DBNAME,
                           help=f"Database name (default: {DEFAULT_DBNAME})")
    argparser.add_argument("-c", "--collsrc", default=DEFAULT_SOURCE_COLLNAME,
                           help=f"Source collection name (default: {DEFAULT_SOURCE_COLLNAME})")
    argparser.add_argument("-o", "--dbtgt", default="",
                           help=f"Target database name (default to same as source database name")
    argparser.add_argument("-t", "--colltgt", default=DEFAULT_TARGET_COLLNAME,
                           help=f"Target collection name (default: {DEFAULT_TARGET_COLLNAME})")
    argparser.add_argument("-s", "--size", default=0, type=int,
                           help=f"Number of documents required (if unset, the number of documents "
                                f"in the target collection will match the number of documents in "
                                f"the source collection)")
    argparser.add_argument("-p", "--pipeline", default="",
                           help=f"The path of the custom aggregation pipeline to apply to the "
                                f"records being generated (default behaviour, if not set, is to not"
                                f" apply a custom pipeline)")
    argparser.add_argument("-z", "--compression", default=DEFAULT_COMPRESSION,
                           choices=["snappy", "zstd", "zlib", "none"],
                           help=f"Collection compression to use (default: {DEFAULT_COMPRESSION})")
    argparser.add_argument("-k", "--shardkey", default="",
                           help=f"For sharded clusters, name of field to use as range shard key "
                                f"for the sharded collection or specify a string of comma "
                                f"separated field names for a range-based compound shard key "
                                f"(default is to use hash sharding instead, with a hash on '_id')")
    args = argparser.parse_args()
    shardKeyElements = []

    if args.shardkey:
        shardKeyElements = [field.strip() for field in args.shardkey.split(',')]

    tgtDbName = args.dbtgt if args.dbtgt else args.dbsrc
    run(args.url, args.dbsrc, args.collsrc, tgtDbName, args.colltgt, args.size, args.compression,
        shardKeyElements, args.pipeline)


##
# Executes the data copying process using intermediate collections to step up the order of
# magnitude of size of the data set.
##
def run(url, srcDbName, srcCollName, tgtDbName, tgtCollName, size, compression, shardKeyFields,
        customPipelineFile):
    print(f"\nConnecting to MongoDB using URL '{url}' "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})\n")
    connection = MongoClient(url)
    adminDB = connection["admin"]
    configDB = connection["config"]
    srcDb = connection[srcDbName]
    tgtDb = connection[tgtDbName]
    mdbVersion = adminDB.command({'buildInfo': 1})['version']
    mdbMajorVersion = int(mdbVersion.split('.')[0])
    originalAmountAvailable = srcDb[srcCollName].count_documents({})
    print(" TIMER: Started timer having now finished counting source collection")
    start = datetime.now()

    # If a source collection is not defined, create an arbitrary collection with one dummy record
    # with just one field ('_id')
    if originalAmountAvailable <= 0:
        srcDb.drop_collection(srcCollName)
        srcDb[srcCollName].insert_one({})
        print(f" WARNING: Created a source collection for '{srcDbName}.{srcCollName}' because it "
              f"doesn't already exist, and added one dummy record with just an '_id' field")
        originalAmountAvailable = 1

    # If no target size specified assume target collection should be same size as source collection
    if size <= 0:
        size = originalAmountAvailable

    # Try to enable sharding and if can't we know its just a simple replica-set
    try:
        adminDB.command("enableSharding", tgtDbName)
        isClusterSharded = True
    except OperationFailure as opFailure:
        if opFailure.code == 13:  # '13' signifies 'authorization' error
            print(f" WARNING: Cannot enable sharding for the database because the specified "
                  f"database user does not have the 'clusterManager' built-in role assigned (or the"
                  f" action privileges to run the 'enablingSharding' and 'splitChunk' commands). If"
                  f" this is an Atlas cluster, you would typically need to assign the 'Atlas Admin'"
                  f" role to the database user.")
        elif opFailure.code != 59:  # '15' signifies 'no such command' error which is fine
            print(f" WARNING: Unable to successfully enable sharding for the database. Error: "
                  f"{opFailure.details}.")

        isClusterSharded = False
    except Exception as e:
        print(" WARNING: Unable to successfully enable sharding for the database. Error: ")
        pprint(e)
        isClusterSharded = False

    rangeShardKeySplits = []

    # If sharded with range based shark key, see if can get a list of pre-split points
    if isClusterSharded and shardKeyFields:
        rangeShardKeySplits = getRangeShardKeySplitPoints(srcDb, srcCollName, shardKeyFields)

    # Create final collection now in case it's sharded and has pre-split chunks - want enough time
    # for balancer to spread out the chunks before it comes under intense ingestion load
    createCollection(adminDB, tgtDb, tgtCollName, compression, isClusterSharded, shardKeyFields,
                     rangeShardKeySplits, size, True)
    print()

    # See how many magnitudes difference there is. For example, source collection may thousands of
    # documents but destination may need to be billions (i.e. 6 order of magnitude difference)
    magnitudesOfDifference = (math.floor(math.log10(size)) -
                              math.ceil(math.log10(originalAmountAvailable)))
    sourceAmountAvailable = originalAmountAvailable
    tempCollectionsToRemove = []
    dbInName = srcDbName
    lastCollSrcName = srcCollName

    # Loop inflating by an order of magnitude each time (if there really is such a difference)
    for magnitudeDifference in range(0, magnitudesOfDifference):
        tmpCollName = f"TEMP_{srcCollName}_{magnitudeDifference}"
        ceilingAmount = (10 ** (math.ceil(math.log10(originalAmountAvailable)) +
                         magnitudeDifference))
        createCollection(adminDB, tgtDb, tmpCollName, compression, isClusterSharded, shardKeyFields,
                         rangeShardKeySplits, ceilingAmount, False)
        copyDataToNewCollection(url, dbInName, tgtDbName, lastCollSrcName, tmpCollName,
                                sourceAmountAvailable, ceilingAmount, customPipelineFile)
        sourceAmountAvailable = ceilingAmount
        tempCollectionsToRemove.append(lastCollSrcName)
        dbInName = tgtDbName
        lastCollSrcName = tmpCollName

    # If target collection uses range shard key pre-spliting, wait for the chunks to be balanced
    if rangeShardKeySplits:
        waitForPresplitChunksToBeBalanced(configDB, tgtDbName, tgtCollName, mdbMajorVersion)

    # Do final inflation to the final collection and print summary
    copyDataToNewCollection(url, dbInName, tgtDbName, lastCollSrcName, tgtCollName,
                            sourceAmountAvailable, size, customPipelineFile)
    tempCollectionsToRemove.append(lastCollSrcName)
    end = datetime.now()
    print(f"Finished database processing work in {int((end-start).total_seconds())} seconds "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})\n")
    print(f"\nNow going to gather & print some summary data + remove old temporary collections\n")
    printSummary(srcDb, tgtDb, srcCollName, tgtCollName, compression)

    # Clean-up any temporary collections that are no longer needed
    if DO_PROPER_RUN:
        if srcCollName in tempCollectionsToRemove:
            tempCollectionsToRemove.remove(srcCollName)  # Ensure not removing original collection

        removeTempCollections(tgtDb, tempCollectionsToRemove)

    print(f"\nEnded ({datetime.now().strftime(DATE_TIME_FORMAT)})\n")


##
# Where source and target size is the same just copy all the data to the source collection,
# otherwise, For a specific order of magnitude expansion, create a new larger source collection
# using data from the source collection.
##
def copyDataToNewCollection(url, srcDbName, tgtDbName, srcCollName, tgtCollName, srcSize, tgtSize,
                            customPipelineFile):
    print(f" COPY. Source size: {srcSize}, target size: {tgtSize}, "
          f"source coll: '{srcCollName}', target coll: '{tgtCollName}' "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})")
    aggBatches = []

    # If source and destination size the same then unlikely to be inflating and more like to be just
    # masking current date with same size output
    if srcSize == tgtSize:
        iterations = min(10, math.floor(tgtSize / 10))  # Want 10 sub-process but not if < 10 docs
        batchSize = math.floor(tgtSize / 10)
        remainder = tgtSize % 10
        skipFactor = 1  # Need to skip cos don't want to duplicate any source data
    else:
        iterations = math.floor(tgtSize / srcSize)  # Will be up to 10 sub-processes
        batchSize = srcSize
        remainder = tgtSize % srcSize
        skipFactor = 0  # Don't want to skip - just using source data to duplication to target

    lastIteration = -1

    for iteration in range(0, iterations):
        lastIteration = iteration
        aggBatches.append(AggBatchMetadata(batchSize, (lastIteration*batchSize*skipFactor)))

    if remainder:
        lastIteration += 1
        aggBatches.append(AggBatchMetadata(remainder, (lastIteration*batchSize*skipFactor)))

    if customPipelineFile and (not isfile(customPipelineFile) or
                               not access(customPipelineFile, R_OK)):
        sys.exit(f"\nERROR: Pipeline file '{customPipelineFile}' does not exist or is not "
                 f"readable.\n")

    if DO_PROPER_RUN:
        print("  |-> ", end="", flush=True)
        spawnBatchProcesses(aggBatches, executeCopyAggPipeline, url, srcDbName, tgtDbName,
                            srcCollName, tgtCollName, customPipelineFile)

    print("\n")


##
# Execute each final aggregation pipeline in its own OS process (hence must re-establish
# MongoClient connection for each process as PyMongo connections can't be shared across processes).
# This function builds and executes an aggregation pipeline which filters out the '_id' field,
# applies the user-provided custom pipeline (if defined) and then performs a merge of the
# aggregation results into a destination collection.
##
def executeCopyAggPipeline(url, srcDbName, tgtDbName, srcCollName, tgtCollName, customPipelineFile,
                           limit, skip):
    print("[", end="", flush=True)
    connection = MongoClient(url, w=MERGE_AGG_WRITE_CONCERN)
    db = connection[srcDbName]

    # Assemble pipeline to filter out '_id' field and merge results into a target collection
    fullPipeline = []

    if skip:
        fullPipeline.append({"$skip": skip})

    if limit:
        fullPipeline.append({"$limit": limit})

    fullPipeline.append(
        {"$unset": [
            "_id"
        ]}
    )

    if customPipelineFile:
        fullPipeline += getPipelineFromPyAggFile(customPipelineFile)

    fullPipeline.append(
        {"$merge": {
            "into": {"db": tgtDbName, "coll": tgtCollName},
            "whenMatched": "fail",
            "whenNotMatched": "insert"
        }}
    )

    db[srcCollName].aggregate(fullPipeline)
    print("]", end="", flush=True)


##
# Analyse the original source collection for its natural split of ranges for the first 2 fields in
# the compound shard key (there may only be one) using the aggregation $bucketAuto operator for each
# field to analyse its spread.
##
def getRangeShardKeySplitPoints(srcDb, srcCollName, shardKeyFields):
    if not shardKeyFields:
        return

    splitPoints = []

    # If range shard key is single field return the split point resulting from analysing it
    if len(shardKeyFields) <= 1:
        splitPoints = getSplitPointsForAField(srcDb, srcCollName, shardKeyFields[0],
                                              TARGET_SPLIT_POINTS_AMOUNT)
    # Otherwise analyse first 2 fields in compound key and merge split points of both together
    # (basically a 'Cartesian product' of the split points of each of the two fields)
    else:
        # We want 256 splits points overall but If we ask for 256 splits for fields 1 and 256 splits
        # for field 2 there will be 256x256=65536 split points. Instead we want the square root
        # quantity for each field, ie. 16x16=256 - so we want 16 for field 1 and 16 for field 2
        targetSplitPointsAmountPerField = math.ceil(math.sqrt(TARGET_SPLIT_POINTS_AMOUNT))
        firstFieldName = shardKeyFields[0]
        secondFieldName = shardKeyFields[1]
        firstFieldsplitPoints = getSplitPointsForAField(srcDb, srcCollName, firstFieldName,
                                                        targetSplitPointsAmountPerField)
        secondFieldsplitPoints = getSplitPointsForAField(srcDb, srcCollName, secondFieldName,
                                                         targetSplitPointsAmountPerField)

        # Produce split points like {"aaa": "XYZ", "bbb": 123} as a Cartesian Product of the splits
        # from the two fields
        for firstFieldPoint in firstFieldsplitPoints:
            for secondFieldPoint in secondFieldsplitPoints:
                combinedSplitPoint = dict(firstFieldPoint)
                combinedSplitPoint.update(secondFieldPoint)
                splitPoints.append(combinedSplitPoint)

    return splitPoints


##
# Analyse the type for a field in the first document in a collection and then analyse the whole
# collection using "$bucketAuto" to get a roughly even range spread of values for the field.
##
def getSplitPointsForAField(db, collName, field, targetSplitPointsAmount):
    # Pipeline to check the type of the field in an existing document and assume all occurrences
    # will have this type
    typePipeline = [
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "type": {"$type": f"${field}"},
        }},
    ]

    firstRecord = db[collName].aggregate(typePipeline).next()
    type = firstRecord["type"]

    # Can't do anything if can't infer type
    if type == "missing":
        sys.exit(f"\nERROR: Shard key field '{field}' is not present in the first document in the"
                 f" source collection '{srcCollName}' and hence cannot be used as part or all of "
                 f"the shard key definition.\n")

    fieldSplitPoints = []

    # Only makes sense to split on specific types (e.g. not boolean which can have only 2 values)
    if type in ["string", "date", "int", "double", "long", "timestamp", "decimal"]:
        splitPointsPipeline = [
            {"$bucketAuto": {
                "groupBy": f"${field}", "buckets": targetSplitPointsAmount
            }},

            {"$group": {
                "_id": "",
                "splitsCount": {"$sum": 1},
                "splitPoints": {
                    "$push": "$_id.min",
                },
            }},

            {"$unset": [
                "_id",
            ]},
        ]

        result = db[collName].aggregate(splitPointsPipeline).next()
        listOfPoints = result["splitPoints"]

        # Sometimes a list entry may be "None" so remove it
        for val in listOfPoints:
            if val is not None:
                # e.g.: {"title": "The Shawshank Redemption"}
                fieldSplitPoints.append({field: val})

    return fieldSplitPoints


##
# Create a collection and make it sharded if we are running against a sharded cluster.
##
def createCollection(adminDB, db, collname, compression, isClusterSharded, shardKeyFields,
                     rangeShardKeySplits, indtendedSize, isFinalCollection):
    dropCollection(db, collname)

    doShardCollection = True if (isClusterSharded and (isFinalCollection or
                                 (indtendedSize >= LARGE_COLLN_COUNT_THRESHOLD))) else False

    # Create the collection a specific compression algorithm
    db.create_collection(collname, storageEngine={"wiredTiger":
                         {"configString": f"block_compressor={compression}"}})

    # If collection is to be sharded need to configure shard key + pre-splitting
    if doShardCollection:  # SHARDED collection
        if shardKeyFields:  # RANGE shard key
            shardKeyFieldsText = ""

            for field in shardKeyFields:
                if shardKeyFieldsText:
                    shardKeyFieldsText += ","

                shardKeyFieldsText += field

            keyFieldOrders = {}

            for field in shardKeyFields:
                keyFieldOrders[field] = 1

            # Configure range based shard key which is pre-split
            adminDB.command("shardCollection", f"{db.name}.{collname}", key=keyFieldOrders)

            if rangeShardKeySplits:
                for splitPoint in rangeShardKeySplits:
                    # print(f"TO {db.name}.{collname} adding middle split point: {splitPoint}")
                    adminDB.command("split", f"{db.name}.{collname}", middle=splitPoint)

                print(f" CREATE. Created collection '{db.name}.{collname}' "
                      f"(compression={compression}) - sharded with range shard key on "
                      f"'{shardKeyFieldsText}' (pre-split)")
            else:
                print(f" CREATE. Created collection '{db.name}.{collname}' "
                      f"(compression={compression}) - sharded with range shard key on "
                      f"{shardKeyFieldsText} (NOT pre-split)")
        else:  # HASH shard key
            # Configure hash based shard key which is pre-split
            adminDB.command("shardCollection", f"{db.name}.{collname}", key={"_id": "hashed"},
                            numInitialChunks=96)
            print(f" CREATE. Created collection '{db.name}.{collname}' (compression={compression})"
                  f" - sharded with hash shard key on '_id' (pre-split)")
    else:  # UNSHARDED collection
        print(f" CREATE. Created collection '{db.name}.{collname}' (compression={compression}) - "
              f"unsharded")


##
# If the target collection is sharded with a range shard key and has been pre-split, wait for the
# chunks to be balanced before subsequently doing inserts, to help maximise subsequent performance.
##
def waitForPresplitChunksToBeBalanced(configDB, dbName, collName, mdbMajorVersion):
    collectionIsImbalanced = True
    shownSleepNotice = False
    waitTimeSecs = 0
    lastChunkCountDifference = -1
    finalConvergenceAttemps = 0
    startTime = datetime.now()
    (aggColl, aggPipeline) = getShardChunksCollAndAggPipeline(dbName, collName, mdbMajorVersion)

    # Periodically run agg pipeline (+ by a sleep) until chunks counts roughly matches on all shards
    while collectionIsImbalanced and (waitTimeSecs < MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS):
        shardsMetadata = configDB[aggColl].aggregate(aggPipeline)
        chunkCounts = []

        for shardMetadata in shardsMetadata:
            # print(f"shard: {shardMetadata['shard']}, chunksCount: {shardMetadata['chunksCount']}")
            chunkCounts.append(shardMetadata["chunksCount"])

        if not chunkCounts:
            print(f" WARNING: Unable to wait for sharded collection to evenly balance because "
                  f"chunks metadata for the collection doesn't to be present - this may indicate a"
                  f" more critical problem")
            return

        chunkCounts.sort()
        lastChunkCountDifference = chunkCounts[-1] - chunkCounts[0]

        if lastChunkCountDifference <= BALANCED_CHUNKS_MAX_DIFFERENCE:
            # If still more than 2 difference, keep trying to get more convergence for a short time
            if (lastChunkCountDifference >= 2) and (finalConvergenceAttemps <= 2):
                finalConvergenceAttemps += 1
            else:
                collectionIsImbalanced = False
                break

        if not shownSleepNotice:
            print(f" WAITING. Waiting for the range key pre-split chunks to balance in the sharded"
                  f" collection '{collName}' - this may take a few minutes")

        time.sleep(BALANCE_CHECK_SLEEP_SECS)
        shownSleepNotice = True
        waitTimeSecs = (datetime.now() - startTime).total_seconds()

    if collectionIsImbalanced:
        print(f" WARNING: Exceeded maximum threshold ({MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS} "
              f"secs) waiting for sharded collection to evenly balance (current difference: "
              f"{lastChunkCountDifference}) - subsequent cluster performance may be degraded "
              f"for a while")
    else:
        print(f" BALANCED. Sharded collection with range key pre-split chunks is now "
              f"balanced (wait time was {waitTimeSecs} secs). Maximum chunk count difference across"
              f" all shards is: {lastChunkCountDifference})")


##
# Returns the pipeline to look at config data & establish chunk count for each shard for sharded
# collection between MongoDB 4.4 and 5.0 the config metadata collections changed in structure hence
# needs to return a different pipeline depending on version.
##
def getShardChunksCollAndAggPipeline(dbName, collName, mdbMajorVersion):
    if mdbMajorVersion <= 4:
        return(("chunks", [
            {"$match": {
                "ns": f"{dbName}.{collName}",
            }},

            {"$group": {
                "_id": "$shard",
                "chunksCount": {"$sum": 1},
            }},

            {"$set": {
                "shard": "$_id",
                "_id": "$$REMOVE",
            }},
        ]))
    else:
        return(("collections", [
            {"$match": {
                "_id": f"{dbName}.{collName}",
            }},

            {"$lookup": {
                "from": "chunks",
                "localField": "uuid",
                "foreignField": "uuid",
                "as": "chunks",
            }},

            {"$unwind": {
                "path": "$chunks",
            }},

            {"$group": {
                "_id": "$chunks.shard",
                "chunksCount": {"$sum": 1},
            }},

            {"$set": {
                "shard": "$_id",
                "_id": "$$REMOVE",
            }},
        ]))


##
# Drop all temporary collections used.
##
def removeTempCollections(db, collectionNames):
    for coll in collectionNames:
        dropCollection(db, coll)


##
# Drop collection.
##
def dropCollection(db, coll):
    print(f" DROP: Removing existing collection: '{db.name}.{coll}'")
    db.drop_collection(coll)


##
# From an external file, load a Python version of a MongoDB Aggregation defined by a variable
# called 'pipeline'
##
def getPipelineFromPyAggFile(filename):
    with open(filename, mode="r") as dataFile, open(f"{PY_IMPORTS_FILE}", mode="r") as importsFile:
        pythonContent = dataFile.read()
        pythonContent = convertJsContentToPy(pythonContent)
        # pprint(pythonContent)
        # Prefix code to make the pipeline variable global to be able to access it afterwards
        pipelineCode = importsFile.read() + "\nglobal pipeline;\n" + pythonContent
        pipelineCompiledCode = compile(pipelineCode, "pipeline", "exec")
        exec(pipelineCompiledCode)
        return pipeline


##
# Convert content of a JavaScript file to Python (based on some limited assumptions about content)
##
def convertJsContentToPy(code):
    pythonContent = code
    pythonContent = re.sub("/\\*.*?\\*/", "", pythonContent, flags=re.DOTALL)  # remove block cmnts
    pythonContent = re.sub(r"//.*?\n", "\n", pythonContent, flags=re.M)  # remove all line comments
    pythonContent = re.sub(r"true", "True", pythonContent, flags=re.M)  # convert js to py bool
    pythonContent = re.sub(r"false", "False", pythonContent, flags=re.M)  # convert js to py bool
    pythonContent = re.sub(r"null", "None", pythonContent, flags=re.M)  # convert js null to py
    return pythonContent


##
# Print summary of source and target collection statistics.
##
def printSummary(srcDb, tgtDb, srcCollName, tgtCollName, compression):
    print("Original collection statistics:")
    printCollectionData(srcDb, srcCollName)
    print("\nFinal collection statistics:")
    printCollectionData(tgtDb, tgtCollName)
    print(f" Compression used: {compression}\n")


##
# Print summary stats for a collection.
##
def printCollectionData(db, collName):
    collstats = db.command("collstats", collName)

    if collstats['size'] > 0:
        print(f" Collection: {db.name}.{collName}")
        print(f" Sharded collection: {'sharded' in collstats}")
        print(f" Average object size: {int(collstats['avgObjSize'])}")
        print(f" Docs amount: {db[collName].count_documents({})}")
        print(f" Docs size (uncompressed): {collstats['size']}")
        print(f" Index size (with prefix compression): {collstats['totalIndexSize']}")
        print(f" Data size (index + uncompressed docs): "
              f"{collstats['size'] + collstats['totalIndexSize']}")
        print(f" Stored data size (docs+indexes compressed): {collstats['totalSize']}")
    else:
        print(f"Collection '{db.name}.{collName}' does not exist")


##
# Spawn multiple process, each running a piece or work in parallel against a batch of records from
# a source collection.
#
# The 'funcToParallelise' argument should have the following signature:
#     myfunc(*args, limit, skip)
# E.g.:
#     myfunc(url, dbName, collName, tgtCollName, customPipelineFile, limit, skip)
##
def spawnBatchProcesses(batches, funcToParallelise, *args):
    processesList = []

    # Create a set of OS processes to perform each batch job in parallel
    for batch in batches:
        process = Process(target=wrapperProcessWithKeyboardException, args=(funcToParallelise,
                          *args, batch.limit, batch.skip))
        processesList.append(process)

    try:
        # Start all processes
        for process in processesList:
            process.start()

        # Wait for all processes to finish
        for process in processesList:
            process.join()
    except KeyboardInterrupt:
        print(f"\nKeyboard interrupted received\n")
        shutdown()


##
# For a newly spawned process, wraps a business function with the catch of a keyboard interrupt to
# then immediately ends the process when the exception occurs without spitting out verbiage.
##
def wrapperProcessWithKeyboardException(*args):
    try:
        args[0](*(args[1:]))
    except OperationFailure as err:
        print("\n\nError occurred when MongoDB Aggregation tried to execute the provided "
              "aggregation pipeline. NOTE: This often occurs if you are using a mask function to "
              "mask some fields from the source input collection but those fields don't exist in "
              "the source collection - first check you have populated the source collection "
              "properly (using mongosh or Compass for example).\n")
        print("ERROR DETAILS:")
        print(err)
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(0)


##
# Swallow the verbiage that is spat out when using 'Ctrl-C' to kill the script.
##
def shutdown():
    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(0)


# Constants
DO_PROPER_RUN = True
LARGE_COLLN_COUNT_THRESHOLD = 100_000_000
TARGET_SPLIT_POINTS_AMOUNT = 512
BALANCED_CHUNKS_MAX_DIFFERENCE = 8
MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS = 800
BALANCE_CHECK_SLEEP_SECS = 5
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
PY_IMPORTS_FILE = "lib/masksFakesGeneraters_py_imports.py"
MERGE_AGG_WRITE_CONCERN = 1
DEFAULT_MONGODB_URL = "mongodb://localhost:27017"
DEFAULT_DBNAME = "test"
DEFAULT_SOURCE_COLLNAME = "testdata"
DEFAULT_TARGET_COLLNAME = "big_collection"
DEFAULT_COMPRESSION = "snappy"


##
# Main
##
if __name__ == "__main__":
    main()
