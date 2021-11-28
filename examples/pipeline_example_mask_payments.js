pipeline = [
    {"$set": {
        "name.first": maskReplaceFirstPart("$name.first", 3),                                   // Obfuscate first 3 chars of first name
        "name.last": maskReplaceLastPart("$name.last", 7),                                      // Obfuscate last 7 chars of first name
        "card_number": maskReplaceFirstPart("$card_number", 12),                                // Obfuscate first 12 chars of 16 digit credit card number
        "card_start": maskAlterDate("$card_start", 30*24*60*60*1000),                           // Change number by +/- 30 days from its current value
        "card_expiry": maskAlterDate("$card_expiry", 60*24*60*60*1000),                         // Change number by +/- 60 days from its current value
        "rank": maskAlterNumber("$rank", 10),                                                   // Change date by +/- 10% of its current value
        "reported": maskAlterBoolean("$reported", 50),                                          // Flip boolean value 50% of the time
        "flagged": maskAlterBoolean("$flagged", 80),                                            // 80% time same boolean value, rest of time the opposite
        "interestRate": maskAlterDecimal("$interestRate", 20),                                  // Change decimal by +/- 20% of its current value
        "balance": maskAlterDecimal("$balance", 10),                                            // Change decimal by +/- 20% of its current value
        "cardType": maskAlterValueFromList("$cardType", 90, ["CREDIT", "DEBIT", "VOUCHER"]),    // 90% time same value, rest of time one of any value
        "classification": maskAlterValueFromList("$classification", 50, ["A+", "B", "C", "D"]), // 60% time same value, rest of time one of any value
        "address.number": maskAlterNumber("$address.number", 10),                               // Change number by +/- 10% of its current value
        "address.street": maskReplaceFirstPart("$address.street", 6),                           // Obfuscate first 6 chars of street name
        "address.town": maskReplaceAll("$address.town"),                                        // Obfuscate all chars (same length string result)
        "address.country": maskReplaceLastPart("$address.country", 5),                          // Obfuscate first 5 chars of street name
        "address.zipcode": maskAlterNumber("$address.zipcode", 10),                             // Change number by +/- 10% of its current value
        "records": maskAlterListFromList("$records", 50, ["X", "Y", "Z"]),                      // Change on average 50% of list members to random values
    }},
]
