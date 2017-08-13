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
        var tableName = EmailReceiver.inferTableNameFromEmailSubject(sesNotification.mail.commonHeaders.subject);

        return this.s3GetObjectAsync(sesNotification.mail.messageId)
            .then(EmailReceiver.extractRawEmailBufferFromS3Object)
            .then(EmailReceiver.extractCsvBufferFromRawEmailBufferAsync)
            .then(EmailReceiver.translateCsvBufferToJsonObjectsAsync)
            .then(EmailReceiver.sanitizeDateTimeProperties)
            .then(EmailReceiver.injectDerivedObjectProperties)
            .then((objects) => EmailReceiver.injectConstantProperties(
                {LastIngestedDateTime: sesNotification.mail.timestamp}, objects))
            .then((objects) => EmailReceiver.translateObjectsToDynamoRequest(tableName, objects))
            .then(this.dynamoBatchWriteAsync.bind(this));
    }

    s3GetObjectAsync(objectKey) {
        return this.s3.getObject({
            Bucket: this.s3bucketName,
            Key: objectKey
        }).promise();
    }

    dynamoBatchWriteAsync(requestParams) {
        console.log('dynamoBatchWrite called with:')
        EmailReceiver.logObject(requestParams);

        return this.dynamodbDocumentClient.batchWrite(requestParams).promise();
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

    static translateObjectsToDynamoRequest(tableName, objects) {
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

    static logObject(object) {
        console.log(JSON.stringify(object, null, 2));
    }
}

function clone(object) {
    return JSON.parse(JSON.stringify(object));
}

module.exports = EmailReceiver;