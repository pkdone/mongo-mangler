// Generate a random date between now and a maximum number of milliseconds from now
function fakeDateAfterNow(maxMillisFromNow) {
    return {
        "$toDate": {"$add": ["$$NOW", {"$multiply": [{"$rand": {}}, maxMillisFromNow]}]}
    };
}


// Generate a random date between a maximum number of milliseconds before now and now
function fakeDateBeforeNow(maxMillisBeforeNow) {
    return {
        "$toDate": {"$subtract": ["$$NOW", {"$multiply": [{"$rand": {}}, maxMillisBeforeNow]}]}
    };    
}


// Generate a whole number up to a maximum number of digits (any more than 15 are ignored)
function fakeNumber(numberOfDigits) {
    return {        
        "$let": {
            "vars": {
                "amountOrZero": {"$max": [1 , numberOfDigits]},
            },   
            "in": {
                "$toLong": {"$substrCP": [{"$toString": {"$toDecimal": {"$rand": {}}}}, 2, {"$min": ["$$amountOrZero", 16]}]}
            }
        }
    };    
}


// Generate a while number between a given minimum and maximum number (inclusive)
function fakeNumberBounded(minNumber, maxNumber) {
    return {        
        "$toLong": {"$add": [minNumber, {"$floor": {"$multiply": [{"$rand": {}}, {"$subtract": [maxNumber, minNumber]}]}}]}
    };    
}


// Generate a text representation of whole number to a specific number of digits (characters) in length
function fakePaddedNumberAsText(numberOfDigits) {
    return {
        "$let": {
            "vars": {
                "amountOrZero": {"$max": [0, numberOfDigits]},
            },   
            "in": {
                "$substrCP": [
                    {"$concat": [
                        {"$substrCP": [{"$toString": {"$toDecimal": {"$rand": {}}}}, 2, 16]},
                        {"$substrCP": [{"$toString": {"$toDecimal": {"$rand": {}}}}, 2, 16]},
                        "0000000000000000000000000000000000000000000000000000000000000001"
                    ]},
                    0,
                    {"$min": ["$$amountOrZero", 30]}
                ]
            }                
        }
    };    
}


// Generate a decimal number between 0.0 and 1.0 with up to 16 decimal places
function fakeDecimal() {
    return {
        "$toDecimal": {"$rand": {}}
    };    
}


// Generate a decimal number with up to a specified number of significant places (e.g. '3' places -> 736.274473638742)
function fakeDecimalSignificantPlaces(maxSignificantPlaces) {
    return {
        "$multiply": [{"$toDecimal": {"$rand": {}}}, {"$pow": [10, {"$min": [maxSignificantPlaces, 16]}]}]
    };    
}


// Generate a True or False value randomly
function fakeBoolean() {
    return {
        "$cond": {
            "if":   {"$lt": [{"$rand": {}}, 0.5]},
            "then": true,
            "else": false,
        }
    };    
}


// Generate a True or False value randomly but where True is likely for a specified percentage of invocations (e.g. 40 -> 40% likely to be True)
function fakeBooleanWeighted(targetAvgPercentTrue) {
    return {
        "$cond": {
            "if":   {"$lte": [{"$rand": {}}, {"$divide": [targetAvgPercentTrue, 100]}]},
            "then": true,
            "else": false,
        }
    };    
}


// Randomly return one value from a provided list
function fakeValueFromList(listOfValues) {
    return {
        "$let": {
            "vars": {
                "values": listOfValues,
            },   
            "in": {   
                "$arrayElemAt": ["$$values", {"$floor": {"$multiply": [{"$size": "$$values"}, {"$rand": {}}]}}]
            }
        }        
    };    
}


// Randomly return one value from a provided list but where values later in the list are more likely to be returned on average
function fakeValueFromListWeighted(listOfValues) {
    return {
        "$let": {
            "vars": {
                "values": listOfValues,
            },   
            "in": {
                "$let": {
                    "vars": {
                        "threshold": {
                            "$floor": {"$multiply": [
                                {"$reduce": {
                                    "input": {"$range": [1, {"$add": [{"$size": "$$values"}, 1]}]},
                                    "initialValue": 0,
                                    "in": {
                                        "$add": ["$$value", "$$this"]
                                    }
                                }},
                                {"$rand": {}},
                            ]}
                        }                          
                    },   
                    "in": {
                        "$getField": {"field": "result", "input": {  // $getField ONLY EXISTS AND WORKS IN MDB v5.0+
                            "$reduce": {
                                "input": {"$range": [0, {"$size": "$$values"}]},
                                "initialValue": {
                                    "accumulator": 0,
                                    "result": null,
                                },
                                "in": {
                                    "accumulator": {"$add": ["$$value.accumulator", "$$this", 1]},
                                    "result": {"$ifNull": ["$$value.result", {
                                        "$cond": {
                                            "if": {"$gte": [{"$add": ["$$value.accumulator", "$$this", 1]}, "$$threshold"]},
                                            "then": {"$arrayElemAt": ["$$values", "$$this"]},
                                            "else": "$$value.result"
                                        }                            
                                    }]},
                                }
                            }
                        }}
                    }
                }
            }
        }
    };    
}


// Generate an array of sub-documents with the specified size, where each item is randomly taken from the input list
function fakeListOfSubDocs(numSumDocs, listOfValues) {
    return {
        "$map": {
            "input": {"$range": [0, numSumDocs]},
            "in": {
                // Have to use $ifNull to do nothing cos 'in:' expects an object so need a no-op expression as object key
                "$ifNull": ["$DUMMY", fakeValueFromList(listOfValues)],
            }
        }
    };
}


// Generate string composed of the same character repeated the specified number of times 
function fakeNChars(char, amount) {
    return {
        "$reduce": {
            "input": {"$range": [0, amount]},
            "initialValue": "",
            "in": {"$concat": ["$$value",  char]}
        } 
    };
}


// Generate a typical first name from an internal pre-defined list of common first names
function fakeFirstName() {
    return fakeValueFromList(["Maria", "Nushi", "Mohammed", "Jose", "Muhammad", "Mohamed", "Wei", "Yan", "John", "David", "Li", "Abdul", "Ana", "Ying", "Michael", "Juan", "Anna", "Mary", "Daniel", "Luis", "Elena", "Marie", "Ibrahim", "Peter", "Sarah", "Xin", "Lin", "Olga"]);
}


// Generate a typical last name from an internal pre-defined list of common last names
function fakeLastName() {
    return fakeValueFromList(["Wang", "Zhang", "Chen", "Singh", "Kumar", "Ali", "Nguyen", "Khan", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzales", "Jackson", "Lee", "Perez", "Thompson"]);
}


// Generate a typical street name from an internal pre-defined list of common street names
function fakeStreetName() {
    return fakeValueFromList(["Station Road", "Park Road", "Church Street", "Victoria Road", "Green Lane", "Manor Road", "Rue de l'Église", "Grande Rue", "Rue du Moulin", "Rue du Château", "Hauptstraße", "Dorfstraße", "Schulstraße", "Bahnhofstraße", "Fourth Street", "Park Street", "Kerkstraat"]);
}


// Generate a typical town name from an internal pre-defined list of common town names
function fakeTownName() {
    return fakeValueFromList(["Bamborourgh", "Forstford", "Irragin", "Shepshed", "Hardersfield", "Beckinsdale", "Swadlincote", "Lunaris", "Accreton", "Cumdivock", "Sirencester", "Ormkirk", "Blencogo", "Llaneybyder", "Haerndean", "Frostford", "Stamford", "Auchendinny", "Stathford"]);
}


// Randomly return the name of one of the countries in the world
function fakeCountryName() {
    return fakeValueFromList(["Australia", "Austria", "Brazil", "Canada", "China", "Egypt", "France", "Germany", "Ghana", "Greece", "India", "Indonesia", "Italy", "Japan", "Malaysia", "Mexico", "Nigeria", "Russia", "Saudi Arabia", "Singapore", "South Africa", "Spain", "Turkey", "United Arab Emirates", "United Kingdom", "United States of America"]);
}


// Generate a random US-style zipcode/postcode (e.g. 10144)
function fakeZipCode() {
    return fakeNumber(5);
}


// Replace the first specified number of characters in a field's value with 'x's
function maskReplaceFirstPart(strOrNum, amount) {
    return {
        "$let": {
            "vars": {
                "text": {"$toString": strOrNum},
                "amountOrZero": {"$max": [0, amount]},
            },   
            "in": {
                "$let": {
                    "vars": {
                        "length": {"$strLenCP": "$$text"},
                        "remainder": {"$subtract": [{"$strLenCP": {"$toString": "$$text"}}, "$$amountOrZero"]},
                    },   
                    "in": {
                        "$concat": [fakeNChars("x", {"$min": ["$$amountOrZero", "$$length"]}), {"$substrCP": ["$$text", "$$amountOrZero", {"$max": ["$$remainder", 0]}]}]                
                    }
                }
            }
        }
    }
}


// Replace the last specified number of characters in a field's value with 'x's
function maskReplaceLastPart(strOrNum, amount) {
    return {
        "$let": {
            "vars": {
                "text": {"$toString": strOrNum},
                "amountOrZero": {"$max": [0, amount]},
            },   
            "in": {
                "$let": {
                    "vars": {
                        "length": {"$strLenCP": "$$text"},
                        "remainder": {"$subtract": [{"$strLenCP": "$$text"}, "$$amountOrZero"]},
                    },   
                    "in": {
                        "$concat": [{"$substrCP": ["$$text", 0, {"$max": ["$$remainder", 0]}]}, fakeNChars("x", {"$min": ["$$amountOrZero", "$$length"]})]                
                    }
                }
            }
        }
    }
}


// Replace all the characters in a field's value with 'x's
function maskReplaceAll(strOrNum) {
    return fakeNChars("x", {"$strLenCP": {"$toString": strOrNum}})
}


// Change the value of a decimal number by adding or taking away a random amount up to a maximum percentage of its current value (e.g. change current value by + or - 10%)
function maskAlterDecimal(currentValue, percent) {
    return {
        "$let": {
            "vars": {
                "fraction": {"$divide": [percent, 100]},
            },   
            "in": {
                "$add": [{"$multiply": [{"$subtract": [{"$rand": {}}, 0.5]}, 2, "$$fraction", currentValue]}, currentValue]
            }
        }
    }
}


// Change the value of a whole number by adding or taking away a random amount up to a maximum percentage of its current value, rounded (e.g. change current value by + or - 10%)
function maskAlterNumber(currentValue, percent) {
    return {
        "$toLong": {"$round": maskAlterDecimal(currentValue, percent)}
    }
}


// Change the value of a datetime by adding or taking away a random amount up to a maximum percentage of its current value (e.g. change current value by + or - 10%)
function maskAlterDate(currentValue, maxChangeMillis) {
    return {
        "$add": [{"$multiply": [{"$subtract": [{"$rand": {}}, 0.5]}, 2, maxChangeMillis]}, currentValue]
    }
}


// Return the same boolean value for a given percentage of time (e.g 40%), and for the rest of the time return the opposite value
function maskAlterBoolean(currentValue, percentSameValue) {
    return {
        "$let": {
            "vars": {
                "fraction": {"$divide": [percentSameValue, 100]},
            },   
            "in": {
                "$cond": {
                    "if":   {"$lte": [{"$rand": {}}, "$$fraction"]},
                    "then": currentValue,
                    "else": {"$not": currentValue},
                }
            }
        }
    };    
}


// Return the same value for a given percentage of time (e.g 40%), and for the rest of the time return a random value from the given list
function maskAlterValueFromList(currentValue, percentSameValue, otherValuesList) {
    return {
        "$let": {
            "vars": {
                "fraction": {"$divide": [percentSameValue, 100]},
            },   
            "in": {
                "$cond": {
                    "if":   {"$lte": [{"$rand": {}}, "$$fraction"]},
                    "then": currentValue,
                    "else": fakeValueFromList(otherValuesList),
                }
            }
        }
    };    
}


// Change on average a given percentage of the list members values to a random value from the provided alternative list
function maskAlterListFromList(currentList, percentSameValues, otherValuesList) {
    return {
        "$let": {
            "vars": {
                "fraction": {"$divide": [percentSameValues, 100]},
            },   
            "in": {
                "$map": {
                    "input": currentList,
                    "as": "item",
                    "in": {
                        "$cond": {
                            "if":   {"$lte": [{"$rand": {}}, "$$fraction"]},
                            "then": "$$item",
                            "else": fakeValueFromList(otherValuesList),
                        }
                    }
                }
            }
        }
    };    
}
