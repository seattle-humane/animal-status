'use strict';

const mailparser = require('mailparser');
const moment = require('moment-timezone')
const nestedCsvParser = require('./nested-csv-parser');;

// This must be assumed/configured - the source data does not contain offset information
const inputTimeZone = 'America/Los_Angeles'

const TABLE_NAME = 'Animals'

const MAX_DYANMO_BATCH_WRITE_SIZE = 25;

class EmailReceiver {
    constructor(s3, s3bucketName, dynamodbDocumentClient) {
        this.s3 = s3;
        this.s3bucketName = s3bucketName;
        this.dynamodbDocumentClient = dynamodbDocumentClient;
    }

    handleEmailNotification(sesNotification) {
        return this.s3GetObjectAsync(sesNotification.mail.messageId)
            .then(EmailReceiver.extractRawEmailBufferFromS3Object)
            .then(EmailReceiver.extractCsvBufferFromRawEmailBufferAsync)
            .then(EmailReceiver.translateCsvBufferToNestedJsonObjectsAsync)
            .then((objects) => EmailReceiver.injectConstantProperties(
                {LastIngestedDateTime: sesNotification.mail.timestamp}, objects))
            .then(EmailReceiver.translateObjectsToDynamoRequests)
            .then(this.dynamoBatchWriteRequestsAsync.bind(this));
    }

    s3GetObjectAsync(objectKey) {
        return this.s3.getObject({
            Bucket: this.s3bucketName,
            Key: objectKey
        }).promise();
    }

    dynamoBatchWriteRequestsAsync(requestParams) {
        console.log(`dynamoBatchWriteRequestsAsync (${requestParams.length} requests)`);
        return Promise.all(requestParams.map(this.dynamoBatchWriteAsync.bind(this)))
    }

    dynamoBatchWriteAsync(singleRequestParam) {
        return this.dynamodbDocumentClient.batchWrite(singleRequestParam).promise();
    }

    static extractRawEmailBufferFromS3Object(s3Object) {
        console.log('extractRawEmailBufferFromS3Object');
        // The s3Object's "Body" refers to the body of the HTTP response from S3, not an email body
        return s3Object.Body;
    }

    static extractCsvBufferFromRawEmailBufferAsync(rawEmailBuffer) {
        console.log(`extractCsvBufferFromRawEmailBufferAsync (email buffer length: ${rawEmailBuffer.length})`);
        return mailparser
            .simpleParser(rawEmailBuffer)
            .then(email => email.attachments[0].content);
    }

    static translateCsvBufferToJsonObjectsAsync(csvBuffer) {
        console.log(`translateCsvBufferToJsonObjectsAsync (csv buffer length: ${csvBuffer.length})`);
        return nestedCsvParser.parseAsync(csvBuffer, {
            mapHeaders: EmailReceiver.sanitizeColumnName,
            mapValue: EmailReceiver.sanitizePropertyValue
        });
    }

    static sanitizeColumnName(name) {
        return name
            .replace(/#/g, "Id")
            .replace(/ of /, ' Of ')
            .replace(/ /g, "")
            .replace(/\//g, "")
            .replace(/\(login\)/g, "")
            .replace(/Sub-location/g, "SubLocation")
            .replace(/ID/g, "Id")
            .replace(/Date$/g, "DateTime");
    }

    static sanitizePropertyValue(originalValue, propertyName) {
        // DynamoDB doesn't allow empty string attributes - they can either be dropped
        // or replaced with a placeholder. We arbitrarily choose the former.
        if (originalValue === '') {
            return null;
        }

        const dateTimePropertyNameRegex = /(DateTime|DateOfBirth)$/;
        if (dateTimePropertyNameRegex.test(propertyName)) {
            return sanitizeDateTime(originalValue);
        }

        return originalValue;
    }

    // Converts from original format to ISO8601 (uses the default moment timezone set at top of file)
    static sanitizeDateTime(originalDateTimeString) {
        return moment.tz(originalDateTimeString, "M/D/YYYY h:m A", inputTimeZone).utc().format();
    }

    static injectConstantProperties(constantProperties, objects) {
        console.log(`injectConstantProperties (${JSON.stringify(constantProperties)} -> ${objects.length} objects)`);
        objects.forEach(o => Object.assign(o, constantProperties));
        return objects;
    }

    static translateObjectsToDynamoRequests(objects) {
        console.log(`translateObjectsToDynamoRequests (${objects.length} objects)`);
        var objectPages = EmailReceiver.paginateArray(objects, MAX_DYANMO_BATCH_WRITE_SIZE);

        console.log("Forming " + objectPages.length + " Dynamo batchWrite request(s) to table " + TABLE_NAME + " for " + objects.length + " object(s)");

        return objectPages.map(page => EmailReceiver.translatePrePagedObjectsToDynamoRequest(TABLE_NAME, page));
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