'use strict';

const mailparser = require('mailparser');
const csvparse = require('csv-parse');
const moment = require('moment-timezone');

// This must be assumed/configured - the source data does not contain offset information
const inputTimeZone = 'America/Los_Angeles'

class EmailReceiver {
    constructor(s3, s3bucketName, dynamodbDocumentClient) {
        this.s3 = s3;
        this.s3bucketName = s3bucketName;
        this.dynamodbDocumentClient = dynamodbDocumentClient;
    }

    handleEmailNotification(sesNotification) {
        var tableName = this.inferTableNameFromEmailSubject(sesNotification.mail.commonHeaders.subject);
        var emailContentPromise = this.s3.getObject({
            Bucket: this.s3bucketName,
            Key: sesNotification.mail.messageId
        }).promise();

        emailContentPromise
            .bind(this)
            .then(this.extractRawEmailBufferFromS3Object)
            .then(this.extractCsvBufferFromRawEmailBufferAsync)
            .then(this.translateCsvBufferToJsonObjectsAsync)
            .then(this.sanitizeDateTimeProperties)
            .then(this.injectDerivedObjectProperties)
            .then((objects) => this.injectConstantProperties(
                {LastIngestedDateTime: sesNotification.mail.timestamp}, objects))
            .then((objects) => this.translateObjectsToDynamoRequest(tableName, objects))
            .then(this.dynamoBatchWrite);
    }

    inferTableNameFromEmailSubject(subject) {
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

    extractRawEmailBufferFromS3Object(s3Object) {
        // The s3Object's "Body" refers to the body of the HTTP response from S3, not an email body
        return s3Object.Body;
    }

    extractCsvBufferFromRawEmailBufferAsync(rawEmailBuffer) {
        return mailparser
            .simpleParser(rawEmailBuffer)
            .then(email => email.attachments[0].content);
    }

    translateCsvBufferToJsonObjectsAsync(csvBuffer) {
        const parseOptions = { columns: this.sanitizeColumnNames.bind(this) }

        return new Promise(function(resolve, reject) {
            return csvparse(csvBuffer, parseOptions, function (err, output) {
                if (err) { reject(err); }
                resolve(output);
            });
        });
    }

    sanitizeColumnNames(columnNames) {
        return columnNames.map(this.sanitizeColumnName);
    }

    sanitizeColumnName(columnName) {
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
    sanitizeDateTime(originalDateTimeString) {
        return moment.tz(originalDateTimeString, "M/D/YYYY h:m A", inputTimeZone).utc().format();
    }

    sanitizeDateTimeProperties(allObjects) {
        const dateTimePropertyNameRegex = /DateTime$/
        return allObjects.map((originalObject) => {
            const newObject = clone(originalObject);
            for (var propertyName in newObject) {
                if (dateTimePropertyNameRegex.test(propertyName)) {
                    newObject[propertyName] = this.sanitizeDateTime(newObject[propertyName]);
                }
            }
            return newObject;
        });
    }

    // This is for DynamoDB's benefit, where we want to make an index based on more than 2 properties
    injectDerivedObjectProperties(allObjects) {
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

    injectConstantProperties(constantProperties, objects) {
        return objects.map((originalObject) =>
            Object.assign(clone(originalObject), constantProperties));
    }

    translateObjectsToDynamoRequest(tableName, objects) {
        var params = { RequestItems: { } };

        params.RequestItems[tableName] = objects.map(o => {
            return { PutRequest: {
                Item: o,
                ConditionExpression: '#OldIngestedDateTime < :NewIngestedDateTime',
                ExpressionAttributeNames: { '#OldIngestedDateTime': 'LastIngestedDateTime' },
                ExpressionAttributeValues: { ':NewIngestedDateTime': o.LastIngestedDateTime }
            } };
        });

        return params;
    }

    dynamoBatchWrite(requestParams) {
        return this.dynamodbDocumentClient.batchWrite(requestParams).promise();
    }

    logObject(object) {
        console.log(JSON.stringify(object, null, 2));
    }
}

function clone(object) {
    return JSON.parse(JSON.stringify(object));
}

module.exports = EmailReceiver;