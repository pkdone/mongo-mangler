// Constants
const DO_FAKE_RATHER_THAN_MASK = true;
const DB = "test";
const COLL = "testdata";
const FAKE_AGG_FILE = "pipeline_example_fake_payments.js";
const MASK_AGG_FILE = "pipeline_example_mask_payments.js";


// Execution
const LIB_FILE = "../lib/masksFakesGeneraters.js";  
const db = db.getSiblingDB(DB);
const coll = db[COLL];

if (DO_FAKE_RATHER_THAN_MASK && (coll.countDocuments() <= 0)) {
    coll.drop();
    coll.insertOne({});
    coll.insertOne({});
}
  
load(LIB_FILE);
load(DO_FAKE_RATHER_THAN_MASK ? FAKE_AGG_FILE : MASK_AGG_FILE);
//print(pipeline);
print(coll.aggregate(pipeline));
