'use strict';

const mailparser = require('mailparser');
const csvparse = require('csv-parse');
const moment = require('moment-timezone');

// This must be assumed/configured - the source data does not contain offset information
const inputTimeZone = 'America/Los_Angeles'

const MAX_DYANMO_BATCH_WRITE_SIZE = 25;

// DynamoDB doesn't allow string attributes to have empty values, but we want
// to be able to distinguish between entries where we haven't ingested a value
// and entries where we've ingested an empty value, so we use this wherever our
// input data has an empty string.
const EMPTY_STRING_PLACEHOLDER = ':::EMPTY_STRING_DYNAMODB_WORKAROUND:::'

class EmailReceiver {
    constructor(s3, s3bucketName, dynamodbDocumentClient) {
        this.s3 = s3;
        this.s3bucketName = s3bucketName;
        this.dynamodbDocumentClient = dynamodbDocumentClient;
    }

    handleEmailNotification(sesNotification) {
        var tableName = EmailReceiver.inferTableNameFromEmailSubject(sesNotification.mail.commonHeaders.subject);

        return this.s3GetObjectAsync(sesNotification.mail.messageId)
            .then(EmailReceiver.extractRawEmailBufferFromS3Object)
            .then(EmailReceiver.extractCsvBufferFromRawEmailBufferAsync)
            .then(EmailReceiver.translateCsvBufferToJsonObjectsAsync)
            .then(EmailReceiver.sanitizeDateTimeProperties)
            .then(EmailReceiver.sanitizeEmptyStringValues)
            .then(EmailReceiver.injectDerivedObjectProperties)
            .then((objects) => EmailReceiver.injectConstantProperties(
                {LastIngestedDateTime: sesNotification.mail.timestamp}, objects))
            .then((objects) => EmailReceiver.translateObjectsToDynamoRequests(tableName, objects))
            .then(this.dynamoBatchWriteRequestsAsync.bind(this));
    }

    s3GetObjectAsync(objectKey) {
        return this.s3.getObject({
            Bucket: this.s3bucketName,
            Key: objectKey
        }).promise();
    }

    dynamoBatchWriteRequestsAsync(requestParams) {
        return Promise.all(requestParams.map(this.dynamoBatchWriteAsync.bind(this)))
    }

    dynamoBatchWriteAsync(singleRequestParam) {
        return this.dynamodbDocumentClient.batchWrite(singleRequestParam).promise();
    }

    static inferTableNameFromEmailSubject(subject) {
        if (subject.search(/AnimalMemos/) != -1) {
            return "AnimalMemos";
        } else if (subject.search(/AnimalHolds/) != -1) {
            return "AnimalHolds";
        } else if (subject.search(/AnimalBehaviorTests/) != -1) {
            return "AnimalBehaviorTests";
        } else if (subject.search(/AnimalPetIds/) != -1) {
            return "AnimalPetIds";
        } else if (subject.search(/Animals/) != -1) {
            // Important that this is last
            return "Animals";
        } else {
            throw new Error("Could not infer table name from subject " + subject);
        }
    }

    static extractRawEmailBufferFromS3Object(s3Object) {
        // The s3Object's "Body" refers to the body of the HTTP response from S3, not an email body
        return s3Object.Body;
    }

    static extractCsvBufferFromRawEmailBufferAsync(rawEmailBuffer) {
        return mailparser
            .simpleParser(rawEmailBuffer)
            .then(email => email.attachments[0].content);
    }

    static translateCsvBufferToJsonObjectsAsync(csvBuffer) {
        const parseOptions = { columns: EmailReceiver.sanitizeColumnNames }

        return new Promise(function(resolve, reject) {
            return csvparse(csvBuffer, parseOptions, function (err, output) {
                if (err) { reject(err); }
                resolve(output);
            });
        });
    }

    static sanitizeColumnNames(columnNames) {
        return columnNames.map(EmailReceiver.sanitizeColumnName);
    }

    static sanitizeColumnName(columnName) {
        return columnName
            .replace(/#/g, "Id")
            .replace(/ /g, "")
            .replace(/\//g, "")
            .replace(/\(login\)/g, "")
            .replace(/Sub-location/g, "SubLocation")
            .replace(/ID/g, "Id")
            .replace(/Date$/g, "DateTime");
    }

    // Converts from original format to ISO8601 (uses the default moment timezone set at top of file)
    static sanitizeDateTime(originalDateTimeString) {
        return moment.tz(originalDateTimeString, "M/D/YYYY h:m A", inputTimeZone).utc().format();
    }

    static sanitizeDateTimeProperties(allObjects) {
        const dateTimePropertyNameRegex = /DateTime$/
        return allObjects.map((originalObject) => {
            const newObject = clone(originalObject);
            for (var propertyName in newObject) {
                if (dateTimePropertyNameRegex.test(propertyName)) {
                    newObject[propertyName] = EmailReceiver.sanitizeDateTime(newObject[propertyName]);
                }
            }
            return newObject;
        });
    }

    static sanitizeEmptyStringValues(allObjects) {
        return allObjects.map((originalObject) => {
            const newObject = clone(originalObject);
            for (var propertyName in newObject) {
                if (newObject[propertyName] === '') {
                    newObject[propertyName] = EMPTY_STRING_PLACEHOLDER;
                }
            }
            return newObject;
        });
    }

    // This is for DynamoDB's benefit, where we want to make an index based on more than 2 properties
    static injectDerivedObjectProperties(allObjects) {
        const derivedPropertyDefinitions = [
            { basePropertyNames: ['BehaviorCategory', 'BehaviorTest'] }
        ];

        return allObjects.map((originalObject) => {
            const newObject = clone(originalObject);
            derivedPropertyDefinitions.forEach((derivedPropertyDefinition) => {
                const basePropertyNames = derivedPropertyDefinition.basePropertyNames;
                if (basePropertyNames.every((baseProperty) => newObject.hasOwnProperty(baseProperty))) {
                    const derivedPropertyName = basePropertyNames.join('-');
                    const derivedPropertyValue = basePropertyNames.map(prop => newObject[prop]).join('-');
                    newObject[derivedPropertyName] = derivedPropertyValue;
                }
            });
            return newObject;
        });
    }

    static injectConstantProperties(constantProperties, objects) {
        return objects.map((originalObject) =>
            Object.assign(clone(originalObject), constantProperties));
    }

    static translateObjectsToDynamoRequests(tableName, objects) {
        var objectPages = EmailReceiver.paginateArray(objects, MAX_DYANMO_BATCH_WRITE_SIZE);

        console.log("Forming " + objectPages.length + " Dynamo batchWrite request(s) to table " + tableName + " for " + objects.length + " object(s)");

        return objectPages.map(page => EmailReceiver.translatePrePagedObjectsToDynamoRequest(tableName, page));
    }

    static translatePrePagedObjectsToDynamoRequest(tableName, prePagedObjects) {
        var params = { RequestItems: { } };
        params.RequestItems[tableName] = prePagedObjects.map(o => {
            if (typeof o.LastIngestedDateTime !== 'string' || o.LastIngestedDateTime.length === 0) {
                throw new Error("Logic error: attempting to PUT object without valid LastIngestedDateTime: " + JSON.stringify(o, null, 2));
            }
            return { PutRequest: {
                Item: o,
                ConditionExpression: '#OldIngestedDateTime < :NewIngestedDateTime',
                ExpressionAttributeNames: { '#OldIngestedDateTime': 'LastIngestedDateTime' },
                ExpressionAttributeValues: { ':NewIngestedDateTime': o.LastIngestedDateTime }
            } };
        });
        return params;
    }

    // paginateArray([1, 2, 3, 4, 5], 2) => [[1, 2], [3, 4], [5]]
    static paginateArray(array, pageSize) {
        var pages = [];
        for(var pageIndex = 0; pageIndex * pageSize < array.length; pageIndex++) {
            pages.push(array.slice(pageIndex * pageSize, Math.min(array.length, (pageIndex + 1) * pageSize)))
        }
        return pages;
    }

    static logObject(object) {
        console.log(JSON.stringify(object, null, 2));
    }
}

function clone(object) {
    return JSON.parse(JSON.stringify(object));
}

module.exports = EmailReceiver;