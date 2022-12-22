pipeline = [
    {"$set": {
        "name.first": fakeFirstName(),                                                         // Random first name
        "name.last": fakeLastName(),                                                           // Random last name
        "card_number": fakePaddedNumberAsText(16),                                             // 16 digit textual credit card number
        "card_start": fakeDateBeforeNow(2*365*24*60*60*1000),                                  // Up to 2 years before now
        "card_expiry": fakeDateAfterNow(365*24*60*60*1000),                                    // Up to 1 year after now
        "rank": fakeNumber(10),                                                                // Any number up to 10 digits in size
        "reported": fakeBoolean(),                                                             // True of False - 50:50
        "flagged": fakeBooleanWeighted(15),                                                    // 15% time True, rest of time False
        "interestRate": fakeDecimal(),                                                         // Random decimal number between 0.0 and 1.0
        "balance": fakeDecimalSignificantPlaces(8),                                            // Random decimal number to 8 significant places
        "cardType": fakeValueFromList(["CREDIT", "DEBIT", "VOUCHER"]),                         // Randomly set to one of CREDIT | DEBIT | VOUCHER
        //"classification": fakeValueFromListWeighted(["A+", "B", "C", "D"]),                  // Random value more likely from later part of list - COMMENTED OUT AS ONLY WORKS IN MDB 5.0+
        "address.number": fakeNumberBounded(1, 99),                                            // Random house number between 1-99
        "address.street": fakeStreetName(),                                                    // Random street name         
        "address.town": fakeTownName(),                                                        // Random town name         
        "address.country": fakeCountryName(),                                                  // Random country name
        "address.zipcode": fakeZipCode(),                                                      // Random zip code
        "safe_location": fakeLocationWithCoordinates(),                                        // Random place with name, id and geo coordinates
        "records": fakeListOfSubDocs(4, ["BLUE", "RED", "GREEN", "PURPLE", "ORANGE", "PINK"]), // Array of 4 random subdocuments
    }},
]
