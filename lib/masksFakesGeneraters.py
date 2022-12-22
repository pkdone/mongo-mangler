# Generate a random date between a start point of millis after 01-Jan-1970 and a maximum set of milliseconds after that date
def fakeDateMillisFromEpoch(startMillis, maxMillis):
    return {
        "$toDate": {"$add": [{"$dateFromString": {"dateString": "1970-01-01"}}, startMillis, {"$multiply": [{"$rand": {}}, maxMillis]}]}
    };



# Generate a random date between now and a maximum number of milliseconds from now
def fakeDateAfterNow(maxMillisFromNow):
    return {
        "$toDate": {"$add": ["$$NOW", {"$multiply": [{"$rand": {}}, maxMillisFromNow]}]}
    };



# Generate a random date between a maximum number of milliseconds before now
def fakeDateBeforeNow(maxMillisBeforeNow):
    return {
        "$toDate": {"$subtract": ["$$NOW", {"$multiply": [{"$rand": {}}, maxMillisBeforeNow]}]}
    };    



# Generate a whole number up to a maximum number of digits (any more than 15 are ignored)
def fakeNumber(numberOfDigits):
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



# Generate a whole number between a given minimum and maximum number (inclusive)
def fakeNumberBounded(minNumber, maxNumber):
    return {        
        "$toLong": {"$add": [minNumber, {"$floor": {"$multiply": [{"$rand": {}}, {"$subtract": [maxNumber, minNumber]}]}}]}
    };    



# Generate a text representation of whole number to a specific number of digits (characters) in length (max 30)
def fakePaddedNumberAsText(numberOfDigits):
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



# Generate a decimal number between 0.0 and 1.0 with up to 16 decimal places
def fakeDecimal():
    return {
        "$toDecimal": {"$rand": {}}
    };    



# Generate a decimal number with up to a specified number of significant places (e.g. '3' places -> 736.274473638742)
def fakeDecimalSignificantPlaces(maxSignificantPlaces):
    return {
        "$multiply": [{"$toDecimal": {"$rand": {}}}, {"$pow": [10, {"$min": [maxSignificantPlaces, 16]}]}]
    };    



# Generate a currency amount with just 2 decimal places and up to a specified number of significant places (e.g. '3' places -> 736.27)
def fakeMoneyAmountDecimal(maxSignificantPlaces):
    return {
        "$round": [fakeDecimalSignificantPlaces(maxSignificantPlaces), 2] 
    };    



# Generate a True or False value randomly
def fakeBoolean():
    return {
        "$cond": {
            "if":   {"$lt": [{"$rand": {}}, 0.5]},
            "then": True,
            "else": False,
        }
    };    



# Generate a True or False value randomly but where True is likely for a specified percentage of invocations (e.g. 40 -> 40% likely to be True)
def fakeBooleanWeighted(targetAvgPercentTrue):
    return {
        "$cond": {
            "if":   {"$lte": [{"$rand": {}}, {"$divide": [targetAvgPercentTrue, 100]}]},
            "then": True,
            "else": False,
        }
    };    



# Return the first value on avererage the specified percentage of invocations otherwise returning second value
def fakeOneOfTwoValuesWeighted(firstVal, secondVal, avgPercentFirstVal):
    return {
        "$cond": {
            "if":   {"$lte": [{"$rand": {}}, {"$divide": [avgPercentFirstVal, 100]}]},
            "then": firstVal,
            "else": secondVal,
        }
    };    



# Randomly return one value from a provided list
def fakeValueFromList(listOfValues):
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



# Randomly return one value from a provided list but where values later in the list are more likely to be returned on average
def fakeValueFromListWeighted(listOfValues):
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
                        "$getField": {"field": "result", "input": {  

                            "$reduce": {
                                "input": {"$range": [0, {"$size": "$$values"}]},
                                "initialValue": {
                                    "accumulator": 0,
                                    "result": None,
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



# Generate an array of sub-documents with the specified size, where each item is randomly taken from the input list
def fakeListOfSubDocs(numSumDocs, listOfValues):
    return {
        "$map": {
            "input": {"$range": [0, numSumDocs]},
            "in": {
                

                "$ifNull": ["$DUMMY", fakeValueFromList(listOfValues)],
            }
        }
    };



# Generate string composed of the same character repeated the specified number of times 
def fakeNSameChars(char, amount):
    return {
        "$reduce": {
            "input": {"$range": [0, amount]},
            "initialValue": "",
            "in": {"$concat": ["$$value",  char]}
        } 
    };



# Generate string composed of random English alphabet uppercase characters repeated the specified number of times 
def fakeNAnyUpperChars(amount):
    return {
        "$reduce": {
            "input": {"$range": [0, amount]},
            "initialValue": "",
            "in": {"$concat": ["$$value",  fakeValueFromList(["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"])]}
        } 
    };



# Generate string composed of random English alphabet lowercase characters repeated the specified number of times 
def fakeNAnyLowerChars(amount):
    return {"$toLower": fakeNAnyUpperChars(amount)};



# Generate a typical first name from an internal pre-defined list of common first names
def fakeFirstName():
    return fakeValueFromList(["Maria", "Nushi", "Mohammed", "Jose", "Muhammad", "Mohamed", "Wei", "Yan", "John", "David", "Li", "Abdul", "Ana", "Ying", "Michael", "Juan", "Anna", "Mary", "Daniel", "Luis", "Elena", "Marie", "Ibrahim", "Peter", "Sarah", "Xin", "Lin", "Olga"]);



# Generate a typical last name from an internal pre-defined list of common last names
def fakeLastName():
    return fakeValueFromList(["Wang", "Zhang", "Chen", "Singh", "Kumar", "Ali", "Nguyen", "Khan", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzales", "Jackson", "Lee", "Perez", "Thompson"]);



# Generate a typical first name and last name from an internal pre-defined list of names
def fakeFirstAndLastName():
    return {"$concat": [fakeFirstName(), " ", fakeLastName()]};



# Generate a random email address with random chars for the email id @ one of a few fixed .com domains
def fakeEmailAddress(idChars):
    return {"$concat": [fakeNAnyLowerChars(idChars), "@", fakeValueFromList(["mymail.com", "fastmail.com", "acmemail.com"])]};



# Generate a random IPv4 address in text format of 'xxx.xxx.xxx.xxx'
def fakeIPAddress():
    return {"$concat": [{"$toString": fakeNumberBounded(0, 255)}, ".", {"$toString": fakeNumberBounded(0, 255)}, ".", {"$toString": fakeNumberBounded(0, 255)}, ".", {"$toString": fakeNumberBounded(0, 255)}]};



# Generate a typical street name from an internal pre-defined list of common street names
def fakeStreetName():
    return fakeValueFromList(["Station Road", "Park Road", "Church Street", "Victoria Road", "Green Lane", "Manor Road", "Rue de l'Église", "Grande Rue", "Rue du Moulin", "Rue du Château", "Hauptstraße", "Dorfstraße", "Schulstraße", "Bahnhofstraße", "Fourth Street", "Park Street", "Kerkstraat"]);



# Generate a typical town name from an internal pre-defined list of common town names
def fakeTownName():
    return fakeValueFromList(["Bamborourgh", "Forstford", "Irragin", "Shepshed", "Hardersfield", "Beckinsdale", "Swadlincote", "Lunaris", "Accreton", "Cumdivock", "Sirencester", "Ormkirk", "Blencogo", "Llaneybyder", "Haerndean", "Frostford", "Stamford", "Auchendinny", "Stathford"]);



# Randomly return the name of one of the countries in the world
def fakeCountryName():
    return fakeValueFromList(["Australia", "Austria", "Brazil", "Canada", "China", "Egypt", "France", "Germany", "Ghana", "Greece", "India", "Indonesia", "Italy", "Japan", "Malaysia", "Mexico", "Nigeria", "Russia", "Saudi Arabia", "Singapore", "South Africa", "Spain", "Turkey", "United Arab Emirates", "United Kingdom", "United States of America"]);



# Generate a random US-style zipcode/postcode (e.g. 10144)
def fakeZipCode():
    return fakeNumber(5);



# Generate a typical company name from an internal pre-defined list of common company names
def fakeCompanyName():
    return fakeValueFromList(["Wonka Industries", "Acme Corp.", "Stark Industries", "Gekko & Co", "Wayne Enterprises", "Cyberdyne Systems", "Genco Pura Olive Oil Company", "Bubba Gump", "Olivia Pope & Associates", "Krusty Krab", "Sterling Cooper", "Soylent", "Hooli", "Good Burger", "Globex Corporation", "Initech", "Umbrella Corporation", "Vehement Capital Partners", "Massive Dynamic"]);



# Generate a random place with typical name, an id and some geo coordinates
def fakeLocationWithCoordinates():
    return fakeValueFromList([
        {"name": "Zoo Station", "id": "327833622", "location": {"type": "Point", "coordinates": [-0.1, 51.5]}},
        {"name": "Shelter Place", "id": "264329372", "location": {"type": "Point", "coordinates": [-73.99, 40.7]}},
        {"name": "Happy Church", "id": "726383926", "location": {"type": "Point", "coordinates": [103.8, 1.33]}},
        {"name": "Tiny Tower", "id": "918273645", "location": {"type": "Point", "coordinates": [151.2, -33.9]}},
        {"name": "Cosy Cavern", "id": "136272621", "location": {"type": "Point", "coordinates": [-51.75, 64.17]}},
        {"name": "Warm Temple", "id": "826473826", "location": {"type": "Point", "coordinates": [-66, -54.9]}},
        {"name": "Peace Corner", "id": "448172639", "location": {"type": "Point", "coordinates": [-7, 62.2]}},
        {"name": "Grassy Cove", "id": "371927611", "location": {"type": "Point", "coordinates": [-5.2, 50]}},
        {"name": "Sub Sanctuary", "id": "484728162", "location": {"type": "Point", "coordinates": [-1.79, 53.3]}},
        {"name": "High Hill", "id": "338817262", "location": {"type": "Point", "coordinates": [-3.27, 53.15]}},
    ]);



# Replace the first specified number of characters in a field's value with 'x's
def maskReplaceFirstPart(strOrNum, amount):
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
                        "$concat": [fakeNSameChars("x", {"$min": ["$$amountOrZero", "$$length"]}), {"$substrCP": ["$$text", "$$amountOrZero", {"$max": ["$$remainder", 0]}]}]                
                    }
                }
            }
        }
    }



# Replace the last specified number of characters in a field's value with 'x's
def maskReplaceLastPart(strOrNum, amount):
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
                        "$concat": [{"$substrCP": ["$$text", 0, {"$max": ["$$remainder", 0]}]}, fakeNSameChars("x", {"$min": ["$$amountOrZero", "$$length"]})]                
                    }
                }
            }
        }
    }



# Replace all the characters in a field's value with 'x's
def maskReplaceAll(strOrNum):
    return fakeNSameChars("x", {"$strLenCP": {"$toString": strOrNum}})



# Change the value of a decimal number by adding or taking away a random amount up to a maximum percentage of its current value (e.g. change current value by + or - 10%)
def maskAlterDecimal(currentValue, percent):
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



# Change the value of a whole number by adding or taking away a random amount up to a maximum percentage of its current value, rounded (e.g. change current value by + or - 10%)
def maskAlterNumber(currentValue, percent):
    return {
        "$toLong": {"$round": maskAlterDecimal(currentValue, percent)}
    }



# Change the value of a datetime by adding or taking away a random amount up to a maximum percentage of its current value (e.g. change current value by + or - 10%)
def maskAlterDate(currentValue, maxChangeMillis):
    return {
        "$add": [{"$multiply": [{"$subtract": [{"$rand": {}}, 0.5]}, 2, maxChangeMillis]}, currentValue]
    }



# Return the same boolean value for a given percentage of time (e.g 40%), and for the rest of the time return the opposite value
def maskAlterBoolean(currentValue, percentSameValue):
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



# Return the same value for a given percentage of time (e.g 40%), and for the rest of the time return a random value from the given list
def maskAlterValueFromList(currentValue, percentSameValue, otherValuesList):
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



# Change on average a given percentage of the list members values to a random value from the provided alternative list
def maskAlterListFromList(currentList, percentSameValues, otherValuesList):
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

