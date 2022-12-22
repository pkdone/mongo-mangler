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
    argparser.add_argument("-i", "--incrfield", default="",
                           help=f"The name of a additional field to be injected into each output "
                                f"document which is an automatically generated monotonically "
                                f"incrementing unique value (default is not create the field; "
                                f"only used if running on MongoDB version 5.0 and greater)")
    args = argparser.parse_args()
    shard_key_elements = []

    if args.shardkey:
        shard_key_elements = [field.strip() for field in args.shardkey.split(',')]

    tgt_db_name = args.dbtgt if args.dbtgt else args.dbsrc
    run(args.url, args.dbsrc, args.collsrc, tgt_db_name, args.colltgt, args.size, args.compression,
        shard_key_elements, args.incrfield, args.pipeline)


##
# Executes the data copying process using intermediate collections to step up the order of
# magnitude of size of the data set.
##
def run(url, src_db_name, src_coll_name, tgt_db_name, tgt_coll_name, size, compression,
        shard_key_fields, incrementing_field_name, custom_pipeline_file):
    print(f"\nConnecting to MongoDB using URL '{url}' "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})\n")
    connection = MongoClient(url)
    admin_db = connection["admin"]
    config_db = connection["config"]
    src_db = connection[src_db_name]
    tgt_db = connection[tgt_db_name]
    mdb_version = admin_db.command({'buildInfo': 1})['version']
    mdb_major_version = int(mdb_version.split('.')[0])
    check_custom_pipeline_file_is_readable(custom_pipeline_file)
    original_collection_is_emtpy = (src_db[src_coll_name].count_documents({}) <= 0)
    print(" TIMER. Started timer having now finished counting source collection")
    start = datetime.now()
    is_cluster_sharded = enable_sharding_if_possible(admin_db, tgt_db_name)

    # If sharded with range based shard key, get list of pre-split points
    range_shard_key_splits = get_split_points_if_range_sharding(url, src_db, tgt_db, src_coll_name,
                                                                size, shard_key_fields,
                                                                custom_pipeline_file,
                                                                is_cluster_sharded)

    # If no src coll, create an arbitrary collection with one dummy record with just 1 f_id ield
    start_amount_available = populate_src_coll_if_empty(src_db, src_coll_name)

    # If no target size specified assume target collection should be same size as source collection
    if size <= 0:
        size = start_amount_available

    # Create final collection now in case it's sharded and has pre-split chunks - want enough time
    # for balancer to spread out the chunks before it comes under intense ingestion load
    create_collection(admin_db, config_db, tgt_db, tgt_coll_name, compression, is_cluster_sharded,
                      shard_key_fields, range_shard_key_splits, size, mdb_major_version, True)
    print()

    # Expands dataset incrementally thru a series of increasing size temp collections if necessary
    temp_colls_to_remove = iterate_inflating_coll(url, admin_db, config_db, src_db, tgt_db,
                                                  src_coll_name, tgt_coll_name,
                                                  start_amount_available, size, is_cluster_sharded,
                                                  shard_key_fields, range_shard_key_splits,
                                                  incrementing_field_name, custom_pipeline_file,
                                                  compression, mdb_major_version)

    # Restore source collection to empty state if it was originally empty
    if original_collection_is_emtpy:
        src_db[src_coll_name].delete_many({})

    # End timer and print summary
    end = datetime.now()
    print(f"Finished database processing work in {int((end-start).total_seconds())} seconds "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})\n")
    print(f"Now going to gather & print summary data (takes a while for large collections)\n")
    print_summary(src_db, tgt_db, src_coll_name, tgt_coll_name, compression)
    print(f"Deleting temporary collections (if any)")
    remove_temp_collections(tgt_db, temp_colls_to_remove, src_coll_name)
    print(f"\nEnded ({datetime.now().strftime(DATE_TIME_FORMAT)})\n")


##
# For each orfder of magnitude jump (if any) copy one temp colleciton to next larger temp
# collection (but starting with source colleciton and ending with destination collection)
##
def iterate_inflating_coll(url, admin_db, config_db, src_db, tgt_db, src_coll_name, tgt_coll_name,
                           start_amount_available, size, is_cluster_sharded, shard_key_fields,
                           range_shard_key_splits, incrementing_field_name, custom_pipeline_file,
                           compression, mdb_major_version):
    # See how many magnitudes difference there is. For example, source collection may be thousands
    # of documents but destination may need to be billions (i.e. 6 order of magnitude difference)
    magnitudes_of_difference = (math.floor(math.log10(size)) -
                                math.ceil(math.log10(start_amount_available)))
    source_amount_available = start_amount_available
    temp_collections_to_remove = []
    db_in_name = src_db.name
    last_coll_src_name = src_coll_name

    # Loop inflating by an order of magnitude each time (if there really is such a difference)
    for magnitude_difference in range(1, magnitudes_of_difference):
        tmp_coll_name = f"TEMP_{src_coll_name}_{magnitude_difference}"
        ceiling_amount = (10 ** (math.ceil(math.log10(start_amount_available)) +
                                 magnitude_difference))
        create_collection(admin_db, config_db, tgt_db, tmp_coll_name, compression,
                          is_cluster_sharded, shard_key_fields, range_shard_key_splits,
                          ceiling_amount, mdb_major_version, False)
        copy_data_to_new_collection(url, db_in_name, tgt_db.name, last_coll_src_name, tmp_coll_name,
                                    source_amount_available, ceiling_amount, custom_pipeline_file,
                                    mdb_major_version)
        source_amount_available = ceiling_amount
        temp_collections_to_remove.append(last_coll_src_name)
        db_in_name = tgt_db.name
        last_coll_src_name = tmp_coll_name

    # If target collection uses range shard key pre-spliting, wait for the chunks to be balanced
    if range_shard_key_splits:
        wait_for_presplit_chunks_to_be_balanced(config_db, tgt_db.name, tgt_coll_name,
                                                mdb_major_version)

    # Do final inflation to the final collection
    copy_data_to_new_collection(url, db_in_name, tgt_db.name, last_coll_src_name, tgt_coll_name,
                                source_amount_available, size, custom_pipeline_file,
                                mdb_major_version, incrementing_field_name)
    temp_collections_to_remove.append(last_coll_src_name)
    return temp_collections_to_remove


##
# Where source and target size is the same just copy all the data to the source collection,
# otherwise, For a specific order of magnitude expansion, create a new larger source collection
# using data from the source collection.
##
def copy_data_to_new_collection(url, src_db_name, tgt_db_name, src_coll_name, tgt_coll_name,
                                src_size, tgt_size, custom_pipeline_file, mdb_major_version,
                                incrementing_field_name=""):
    print(f" COPY. Source size: {src_size}, target size: {tgt_size}, source db coll: "
          f"'{src_db_name}.{src_coll_name}', target db coll: '{tgt_db_name}.{tgt_coll_name}' "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})")
    agg_batches = []

    # If source and destination size the same then unlikely to be inflating and more like to be just
    # masking current date with same size output
    if src_size == tgt_size:
        iterations = min(10, math.floor(tgt_size / 10))  # Want 10 sub-process but not if < 10 docs
        batch_size = math.floor(tgt_size / 10)
        remainder = tgt_size % 10
        skip_factor = 1  # Need to skip cos don't want to duplicate any source data
    else:
        iterations = math.floor(tgt_size / src_size)  # Normally <-10 sub-processes (can be <=99)
        batch_size = src_size
        remainder = tgt_size % src_size
        skip_factor = 0  # Don't want to skip - just using source data to duplicate to target

    last_iteration = -1

    for iteration in range(0, iterations):
        last_iteration = iteration
        agg_batches.append(AggBatchMetadata(batch_size, (last_iteration*batch_size*skip_factor)))

    if remainder:
        last_iteration += 1
        agg_batches.append(AggBatchMetadata(remainder, (last_iteration*batch_size*skip_factor)))

    if DO_PROPER_RUN:
        print(" |-> ", end="", flush=True)
        spawn_batch_processes(agg_batches, execute_copy_agg_pipeline, url, src_db_name,
                              tgt_db_name, src_coll_name, tgt_coll_name, custom_pipeline_file,
                              incrementing_field_name, mdb_major_version, batch_size)

    print("\n")


##
# See if can read custom agg pipeline file
##
def check_custom_pipeline_file_is_readable(custom_pipeline_file):
    if custom_pipeline_file and (not isfile(custom_pipeline_file) or
                                 not access(custom_pipeline_file, R_OK)):
        sys.exit(f"\nERROR. Pipeline file '{custom_pipeline_file}' does not exist or is not "
                 f"readable.\n")


##
# Execute each final aggregation pipeline in its own OS process (hence must re-establish
# MongoClient connection for each process as PyMongo connections can't be shared across processes).
# This function builds and executes an aggregation pipeline which filters out the '_id' field,
# applies the user-provided custom pipeline (if defined) and then performs a merge of the
# aggregation results into a destination collection.
##
def execute_copy_agg_pipeline(url, src_db_name, tgt_db_ame, src_coll_name, tgt_coll_name,
                              custom_pipeline_file, incrementing_field_name, mdb_major_version,
                              batch_size, limit, skip, iteration):
    print("[", end="", flush=True)
    connection = MongoClient(url, w=MERGE_AGG_WRITE_CONCERN)
    db = connection[src_db_name]

    # Assemble pipeline to filter out '_id' field and merge results into a target collection
    full_pipeline = []

    # Add skip stage
    if skip:
        full_pipeline.append({"$skip": skip})

    # Add limit stage
    if limit:
        full_pipeline.append({"$limit": limit})

    if incrementing_field_name and (mdb_major_version >= 5):
        # Add stage to increment new field value for each record in the batch
        full_pipeline.append(
            {"$setWindowFields": {
                "sortBy": {},
                "output": {
                    incrementing_field_name: {
                        "$count": {},
                        "window": {
                            "documents": ["unbounded", "current"]
                        }
                    }
                }
            }}
        )

        # Add stage to offset new field's value to ensure it is unique accross all batches, plus
        # deduct 1 from the value because $setWindows$count above starts at 1 not 0
        full_pipeline.append(
            {"$set": {
                incrementing_field_name: {"$sum": [
                    f"${incrementing_field_name}",
                    ((iteration * batch_size) - 1)
                ]},
            }}
        )

    # Add stage to remove old _id which would otherwise risk being a duplicate
    full_pipeline.append(
        {"$unset": [
            "_id"
        ]}
    )

    # Add the custom pipeline part (if any)
    if custom_pipeline_file:
        full_pipeline += get_pipeline_from_py_agg_file(custom_pipeline_file)

    # Add the final merge stage to enable pipeline outputted documents to go target collections
    full_pipeline.append(
        {"$merge": {
            "into": {"db": tgt_db_ame, "coll": tgt_coll_name},
            "whenMatched": "fail",
            "whenNotMatched": "insert"
        }}
    )

    db[src_coll_name].aggregate(full_pipeline)
    print("]", end="", flush=True)


##
# If sharded with range based shard key, see if can get a list of pre-split points and if no source
# collection then generate a temporary collection with some temporary data to be able to sample it
##
def get_split_points_if_range_sharding(url, src_db, tgt_db, src_coll_name, size, shard_key_fields,
                                       custom_pipeline_file, is_cluster_sharded):
    range_shard_key_splits = []

    if is_cluster_sharded and shard_key_fields:
        (check_db, check_coll_name) = create_tmp_sample_coll_if_necessary(url, src_db, tgt_db,
                                                                          src_coll_name,
                                                                          custom_pipeline_file,
                                                                          size)
        range_shard_key_splits = get_range_shard_key_split_points(check_db, check_coll_name,
                                                                  shard_key_fields, size)

        if check_coll_name != src_coll_name:  # Remove temp collection if it exists
            check_db.drop_collection(check_coll_name)

    return range_shard_key_splits


##
# Create a temporary collection if source collection has no records and then populate if with some
# data by running the custom pipeline so that subsequent shard range key sampling has some data to
# work off.
##
def create_tmp_sample_coll_if_necessary(url, src_db, tgt_db, src_coll_name, custom_pipeline_file,
                                        size):
    if is_collection_empty_or_only_one_with_just_an_id_field(src_db, src_coll_name):
        dummy_coll_name = f"TEMP_{src_coll_name}_DUMMY"
        tmp_coll_name = f"TEMP_{src_coll_name}_SAMPLE"
        print(f" SAMPLE-GENERATION. Needing to create a sharded collection with range key but "
              f"source collection has no records to sample to determine the split points. Therefore"
              f" creating a temporary collection to populate some records before trying to sample "
              f"it (and deleting this temporary collection immediately afterwards). Temporary "
              f"collection: {tgt_db.name}.{tmp_coll_name}'")
        tgt_db.drop_collection(dummy_coll_name)
        tgt_db.drop_collection(tmp_coll_name)
        tgt_db[dummy_coll_name].insert_one({})
        sample_amount = min(size+1, MAX_TMP_SAMPLE_AMOUNT)
        copy_data_to_new_collection(url, tgt_db.name, tgt_db.name, dummy_coll_name, tmp_coll_name,
                                    1, sample_amount, custom_pipeline_file, -1)
        tgt_db.drop_collection(dummy_coll_name)
        return (tgt_db, tmp_coll_name)
    else:
        return (src_db, src_coll_name)


##
# Analyse the original source collection for its natural split of ranges for the first 2 fields in
# the compound shard key (there may only be one) using the aggregation $bucketAuto operator for each
# field to analyse its spread.
##
def get_range_shard_key_split_points(src_db, src_coll_name, shard_key_fields, size):
    if not shard_key_fields:
        return

    split_points_target_amount = (TARGET_SPLIT_POINTS_AMOUNT if size >= LARGE_COLLN_COUNT_THRESHOLD
                                  else TARGET_SPLIT_POINTS_AMOUNT / 2)
    split_points = []

    # If range shard key is single field return the split point resulting from analysing it
    if len(shard_key_fields) <= 1:
        split_points = get_split_points_for_a_field(src_db, src_coll_name, shard_key_fields[0],
                                                    split_points_target_amount)
    # Otherwise analyse first 2 fields in compound key and merge split points of both together
    # (basically a 'Cartesian product' of the split points of each of the two fields)
    else:
        # We want 256 splits points overall but If we ask for 256 splits for fields 1 and 256 splits
        # for field 2 there will be 256x256=65536 split points. Instead we want the square root
        # quantity for each field, ie. 16x16=256 - so we want 16 for field 1 and 16 for field 2
        target_split_points_amnt_per_field = math.ceil(math.sqrt(split_points_target_amount))
        first_field_name = shard_key_fields[0]
        second_field_name = shard_key_fields[1]
        first_field_split_points = get_split_points_for_a_field(src_db, src_coll_name,
                                                                first_field_name,
                                                                target_split_points_amnt_per_field)
        second_field_split_points = get_split_points_for_a_field(src_db, src_coll_name,
                                                                 second_field_name,
                                                                 target_split_points_amnt_per_field)

        # Produce split points like {"aaa": "XYZ", "bbb": 123} as a Cartesian Product of the splits
        # from the two fields
        for first_field_point in first_field_split_points:
            for second_field_point in second_field_split_points:
                combined_split_point = dict(first_field_point)
                combined_split_point.update(second_field_point)
                split_points.append(combined_split_point)

    return split_points


##
# Analyse the type for a field in the first document in a collection and then analyse the whole
# collection using "$bucketAuto" to get a roughly even range spread of values for the field.
##
def get_split_points_for_a_field(db, coll_name, field, target_split_points_amount):
    # Pipeline to check the type of the field in an existing document and assume all occurrences
    # will have this type
    type_pipeline = [
        {"$limit": 1},
        {"$project": {
            "_id": 0,
            "type": {"$type": f"${field}"},
        }},
    ]

    first_record = db[coll_name].aggregate(type_pipeline).next()
    data_type = first_record["type"]

    # Can't do anything if can't infer type
    if data_type == "missing":
        sys.exit(f"\nERROR. Shard key field '{field}' is not present in the first document in the"
                 f" source db collection '{db.name}.{coll_name}' and hence cannot be used as part"
                 f" or all of the shard key definition.\n")

    field_split_points = []

    # Only makes sense to split on specific types (e.g. not boolean which can have only 2 values)
    if data_type in ["string", "date", "int", "double", "long", "timestamp", "decimal"]:
        split_points_pipeline = [
            {"$bucketAuto": {
                "groupBy": f"${field}", "buckets": target_split_points_amount
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

        result = db[coll_name].aggregate(split_points_pipeline).next()
        list_of_points = result["splitPoints"]

        # Sometimes a list entry may be "None" so remove it
        for val in list_of_points:
            if val is not None:
                # e.g.: {"title": "The Shawshank Redemption"}
                field_split_points.append({field: val})

    return field_split_points


##
# Create a collection and make it sharded if we are running against a sharded cluster.
##
def create_collection(admin_db, config_db, db, coll_name, compression, is_cluster_sharded,
                      shard_key_fields, range_shard_key_splits, indtended_size, mdb_major_version,
                      is_final_collection):
    drop_collection(db, coll_name)
    do_shard_collection = True if (is_cluster_sharded and (is_final_collection or
                                   (indtended_size >= LARGE_COLLN_COUNT_THRESHOLD))) else False

    # Create the collection a specific compression algorithm
    db.create_collection(coll_name, storageEngine={"wiredTiger":
                         {"configString": f"block_compressor={compression}"}})

    # If collection is to be sharded need to configure shard key + pre-splitting
    if do_shard_collection:  # SHARDED collection
        if shard_key_fields:  # RANGE shard key
            shard_collection_with_range_key(admin_db, config_db, db, coll_name, compression,
                                            shard_key_fields, range_shard_key_splits,
                                            mdb_major_version)
        else:  # HASH shard key
            # Configure hash based shard key which is pre-split
            shard_collection_with_hash_key(admin_db, db, coll_name, compression)
    else:  # UNSHARDED collection
        print(f" CREATE. Created collection '{db.name}.{coll_name}' (compression={compression}) - "
              f"unsharded")


##
# Run the admin commands to shard the collection using a range based shard key, and if possible,
# try to pre-split its chunks.
##
def shard_collection_with_range_key(admin_db, config_db, db, coll_name, compression,
                                    shard_key_fields, range_shard_key_splits, mdb_major_version):
    # Configure range based shard key which is pre-split
    shard_key_fields_text, key_ordered_fields_doc = get_shard_key_fields_metadata(shard_key_fields)
    admin_db.command("shardCollection", f"{db.name}.{coll_name}", key=key_ordered_fields_doc)

    if range_shard_key_splits:
        logged_split_error = False
        count = 0
        shard_names = get_shard_names(config_db)

        for split_point in range_shard_key_splits:
            try:
                # print(f"TO {db.name}.{coll_name} adding middle split point: {split_point}")
                admin_db.command("split", f"{db.name}.{coll_name}", middle=split_point)
                move_chunk_if_mdb60_or_greater(mdb_major_version, admin_db, db, coll_name,
                                               shard_names, count, split_point)
            except Exception:
                if not logged_split_error:
                    print(f" WARNING: Server error occurred trying to split on the specific chunk "
                          f"boundary '{split_point}', but continuing with other splits (won't "
                          f"show this warning again for this run)")
                    logged_split_error = True

            count += 1

        print(f" CREATE. Created collection '{db.name}.{coll_name}' (compression={compression}) - "
              f"sharded with range shard key on '{shard_key_fields_text}' (pre-split into "
              f"{len(range_shard_key_splits)} parts)")
    else:
        print(f" CREATE. Created collection '{db.name}.{coll_name}' (compression={compression}) - "
              f"sharded with range shard key on {shard_key_fields_text} (NOT pre-split)")


##
# In MongoDB 6.0, if chunks are empty, need to explicitly ask to move them.
##
def move_chunk_if_mdb60_or_greater(mdb_major_version, admin_db, db, coll_name, shard_names, count,
                                   split_point):
    if mdb_major_version >= 6:
        if count < 1:
            print(f" WAITING. Creating each range key pre-split chunk in the collection "
                  f"'{coll_name}', which takes a while to complete to alllow the chunks to be "
                  f"balanced out evenly across all the shards")

        shard_name = shard_names[count % len(shard_names)]
        admin_db.command("moveChunk", f"{db.name}.{coll_name}", find=split_point, to=shard_name,
                         _waitForDelete=True)


##
# For a a set of shard key fields, build a sorted doc representation and a text representation.
##
def get_shard_key_fields_metadata(shard_key_fields):
    shard_key_fields_text = ""

    for field in shard_key_fields:
        if shard_key_fields_text:
            shard_key_fields_text += ","

        shard_key_fields_text += field

    key_ordered_fields_doc = {}

    for field in shard_key_fields:
        key_ordered_fields_doc[field] = 1

    return shard_key_fields_text, key_ordered_fields_doc


##
# Run the admin commands to shard the collection using a hash based shard key telling the system to
# automatically pre-split the chunks
##
def shard_collection_with_hash_key(admin_db, db, coll_name, compression):
    admin_db.command("shardCollection", f"{db.name}.{coll_name}", key={"_id": "hashed"},
                     numInitialChunks=96)
    print(f" CREATE. Created collection '{db.name}.{coll_name}' (compression={compression})"
          f" - sharded with hash shard key on '_id' (pre-split)")


##
# If the target collection is sharded with a range shard key and has been pre-split, wait for the
# chunks to be balanced before subsequently doing inserts, to help maximise subsequent performance.
# This only applies for MongoDB versions before 6.0, because in 6.0 onwards the way splitting and
# balancing chunks has changed, which was catered for in this utility when the collection was
# created.
##
def wait_for_presplit_chunks_to_be_balanced(config_db, db_name, coll_name, mdb_major_version):
    if mdb_major_version >= 6:
        return  # If MDB 6+ then don't do this check

    collection_is_imbalanced = True
    shown_sleep_notice = False
    wait_time_secs = 0
    last_chunk_count_difference = -1
    final_convergence_attemps = 0
    start_time = datetime.now()
    (agg_coll, agg_pipeline) = get_shard_chunks_coll_and_agg_pipeline(db_name, coll_name,
                                                                      mdb_major_version)

    # Periodically run agg pipeline (+ a sleep) until chunks counts roughly matches on all shards
    while collection_is_imbalanced and (wait_time_secs < MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS):
        if not shown_sleep_notice:
            print(f" WAITING. Waiting for the range key pre-split chunks to balance in the sharded"
                  f" collection '{coll_name}' - this may take a few minutes")

        shown_sleep_notice = True
        time.sleep(BALANCE_CHECK_SLEEP_SECS)
        chunk_counts = get_current_chunks_count_per_shard(config_db, agg_coll, agg_pipeline)

        # If no chunks or only chunks for one shard, warn and return
        if warn_if_chunks_problem_and_return(chunk_counts):
            return

        # Only compare of chunks have spread out over 2 or more shards (otherwise have to keep
        # waiting for another iteration)
        if len(chunk_counts) >= 2:
            chunk_counts.sort()
            last_chunk_count_difference = chunk_counts[-1] - chunk_counts[0]

            if last_chunk_count_difference <= BALANCED_CHUNKS_MAX_DIFFERENCE:
                # If still > 2 difference, keep trying to get more convergence for a short time
                if (last_chunk_count_difference >= 2) and (final_convergence_attemps <= 2):
                    final_convergence_attemps += 1
                else:
                    collection_is_imbalanced = False
                    break

        wait_time_secs = (datetime.now() - start_time).total_seconds()

    print_chunks_balancing_summary(collection_is_imbalanced, last_chunk_count_difference,
                                   wait_time_secs)


##
# Get the current number of chunks on each shard
##
def get_current_chunks_count_per_shard(config_db, agg_coll, agg_pipeline):
    shards_metadata = config_db[agg_coll].aggregate(agg_pipeline)
    chunk_counts = []

    for shard_metadata in shards_metadata:
        # print(f"shard: {shardMetadata['shard']}, chunksCount: {shardMetadata['chunksCount']}")
        chunk_counts.append(shard_metadata["chunksCount"])

    return chunk_counts


##
# Get names of deployed shards
##
def get_shard_names(config_db):
    pipeline = [
        {"$project": {
            "_id": 0,
            "name": "$_id",
        }},
    ]

    shards_metadata = config_db["shards"].aggregate(pipeline)
    shard_names = [shard["name"] for shard in shards_metadata]
    return shard_names


##
# If no chunks or only chunks for one shard, warn and return True to say caller should return
##
def warn_if_chunks_problem_and_return(chunk_counts):
    if not chunk_counts:
        print(f" WARNING. Unable to wait for sharded collection to evenly balance because chunks "
              f"metadata for the collection doesn't seem to be present - this may indicate a more "
              f"critical problem")
        return True

    return False


##
# Print info on successful or unsuccessfull chunk rebalancing
##
def print_chunks_balancing_summary(collection_is_imbalanced, last_chunk_count_difference,
                                   wait_time_secs):
    if collection_is_imbalanced:
        print(f" WARNING. Exceeded maximum threshold ({MAX_WAIT_TIME_FOR_CHUNKS_BALANCE_SECS} "
              f"secs) waiting for sharded collection to evenly balance (current difference: "
              f"{last_chunk_count_difference}) - subsequent cluster performance may be degraded "
              f"for a while")
    else:
        print(f" BALANCED. Sharded collection with range key pre-split chunks is now "
              f"balanced (wait time was {wait_time_secs} secs). Maximum chunk count difference "
              f"across all shards is: {last_chunk_count_difference}")


##
# If a source collection is not defined, create an arbitrary collection with one dummy record with
# 'just one field ('_id').
##
def populate_src_coll_if_empty(src_db, src_coll_name):
    original_amount_available = src_db[src_coll_name].count_documents({})

    if original_amount_available <= 0:
        src_db[src_coll_name].insert_one({})
        print(f" WARNING. Created a source collection for '{src_db.name}.{src_coll_name}' because "
              f"it doesn't already exist (or has no data), and added one dummy record with just an"
              f" '_id' field")
        original_amount_available = 1

    return original_amount_available


##
# Clean-up any temporary collections that are no longer needed but don't delete source collection.
##
def remove_temp_collections(db, temp_colls_to_remove, src_coll_name):
    if DO_PROPER_RUN:
        for coll_name in temp_colls_to_remove:
            if coll_name != src_coll_name:
                drop_collection(db, coll_name)


##
# Returns the pipeline to look at config data & establish chunk count for each shard for sharded
# collection between MongoDB 4.4 and 5.0 the config metadata collections changed in structure hence
# needs to return a different pipeline depending on version.
##
def get_shard_chunks_coll_and_agg_pipeline(db_name, coll_name, mdb_major_version):
    if mdb_major_version <= 4:
        return("chunks", [
            {"$match": {
                "ns": f"{db_name}.{coll_name}",
            }},

            {"$group": {
                "_id": "$shard",
                "chunksCount": {"$sum": 1},
            }},

            {"$set": {
                "shard": "$_id",
                "_id": "$$REMOVE",
            }},
        ])
    else:
        return("collections", [
            {"$match": {
                "_id": f"{db_name}.{coll_name}",
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
        ])


##
# Try to enable sharding and if can't we know its just a simple replica-set
##
def enable_sharding_if_possible(admin_db, db_name):
    is_cluster_sharded = False

    try:
        admin_db.command("enableSharding", db_name)
        is_cluster_sharded = True
    except OperationFailure as opFailure:
        if opFailure.code == 13:  # '13' signifies 'authorization' error
            print(f" WARNING. Cannot enable sharding for the database because the specified "
                  f"database user does not have the 'clusterManager' built-in role assigned (or the"
                  f" action privileges to run the 'enablingSharding' and 'splitChunk' commands). If"
                  f" this is an Atlas cluster, you would typically need to assign the 'Atlas Admin'"
                  f" role to the database user.")
        elif (opFailure.code != 15) and (opFailure.code != 59):  # Code for 'no such command' so OK
            print(f" WARNING. Unable to successfully enable sharding for the database. Error: "
                  f"{opFailure.details}.")
    except Exception as e:
        print(" WARNING. Unable to successfully enable sharding for the database. Error: ")
        pprint(e)

    return is_cluster_sharded


##
# Check if the source collection is just a dummy collection created from a previous run of this tool
# that failed to clean up due to an error (i.e. only a single record exists in the collection and it
# only had one field which is the _id field).
##
def is_collection_empty_or_only_one_with_just_an_id_field(db, coll_name):
    first_record = None
    record_count = 0
    records = db[coll_name].find().limit(2)

    for record in records:
        if not first_record:
            first_record = record

        record_count += 1

    if (record_count <= 0) or ((record_count == 1) and (len(first_record.keys()) <= 1)):
        return True

    return False


##
# Drop collection.
##
def drop_collection(db, coll_name):
    print(f" DROP. Removing existing collection: '{db.name}.{coll_name}'")
    db.drop_collection(coll_name)


##
# From an external file, load a Python version of a MongoDB Aggregation defined by a variable
# called 'pipeline'
##
def get_pipeline_from_py_agg_file(filename):
    with open(filename, mode="r") as dataFile, open(f"{PY_IMPORTS_FILE}", mode="r") as importsFile:
        python_content = dataFile.read()
        python_content = convert_js_content_to_py(python_content)
        # pprint(pythonContent)
        # Prefix code to make the pipeline variable global to be able to access it afterwards
        pipeline_code = importsFile.read() + "\nglobal pipeline;\n" + python_content
        pipeline_compiled_code = compile(pipeline_code, "pipeline", "exec")
        exec(pipeline_compiled_code)
        return pipeline  # Ignore linter warnings of being undefined as is pulled in via exec()


##
# Convert content of a JavaScript file to Python (based on some limited assumptions about content)
##
def convert_js_content_to_py(code):
    python_content = code
    python_content = re.sub(r"/\*.*?\*/", "", python_content, flags=re.DOTALL)  # remove blkcmts
    python_content = re.sub(r"//[^\n]*", "\n", python_content, flags=re.M)  # remove all line cmnts
    python_content = python_content.replace("true", "True")  # convert js to py bool
    python_content = python_content.replace(r"false", "False")  # convert js to py bool
    python_content = python_content.replace(r"null", "None")  # convert js null to py
    return python_content


##
# Print summary of source and target collection statistics.
##
def print_summary(src_db, tgt_db, src_coll_name, tgt_coll_name, compression):
    print("Original collection statistics:")
    print_collection_data(src_db, src_coll_name)
    print("\nFinal collection statistics:")
    print_collection_data(tgt_db, tgt_coll_name)
    print(f" Compression used: {compression}\n")


##
# Print summary stats for a collection.
##
def print_collection_data(db, coll_name):
    collstats = db.command("collstats", coll_name)

    if collstats['size'] > 0:
        print(f" Collection: {db.name}.{coll_name}")
        print(f" Sharded collection: {'sharded' in collstats}")
        print(f" Average object size: {int(collstats['avgObjSize'])}")
        print(f" Docs amount: {db[coll_name].count_documents({})}")
        print(f" Docs size (uncompressed): {collstats['size']}")
        print(f" Index size (with prefix compression): {collstats['totalIndexSize']}")
        print(f" Data size (index + uncompressed docs): "
              f"{collstats['size'] + collstats['totalIndexSize']}")
        print(f" Stored data size (docs+indexes compressed): {collstats['totalSize']}")
    else:
        print(f" Collection '{db.name}.{coll_name}' does not exist or have any data")


##
# Spawn multiple process, each running a piece or work in parallel against a batch of records from
# a source collection.
#
# The 'funcToParallelise' argument should have the following signature:
#     myfunc(*args, limit, skip)
# E.g.:
#     myfunc(url, dbName, coll_Name, tgt_collnName, custom_pipeline_file, limit, skip)
##
def spawn_batch_processes(batches, func_to_parallelise, *args):
    processes_list = []
    iteration = 0

    # Create a set of OS processes to perform each batch job in parallel
    for batch in batches:
        process = Process(target=wrapper_process_with_keyboard_exception, args=(func_to_parallelise,
                          *args, batch.limit, batch.skip, iteration))
        processes_list.append(process)
        iteration += 1

    try:
        # Start all processes
        for process in processes_list:
            process.start()

        # Wait for all processes to finish
        for process in processes_list:
            process.join()
    except KeyboardInterrupt:
        print(f"\nKeyboard interrupted received\n")
        shutdown()


##
# For a newly spawned process, wraps a business function with the catch of a keyboard interrupt to
# then immediately ends the process when the exception occurs without spitting out verbiage.
##
def wrapper_process_with_keyboard_exception(*args):
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
    except SystemExit:
        os._exit(0)


# Constants
DO_PROPER_RUN = True
LARGE_COLLN_COUNT_THRESHOLD = 100_000_000
TARGET_SPLIT_POINTS_AMOUNT = 512
MAX_TMP_SAMPLE_AMOUNT = 64
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
