'use strict';

const mailparser = require('mailparser');
const moment = require('moment-timezone')
const stripBomBuf = require('strip-bom-buf');

const nestedCsvParser = require('./nested-csv-parser');;

// This must be assumed/configured - the source data does not contain offset information
const inputTimeZone = 'America/Los_Angeles'

const TABLE_NAME = 'Animals'

class EmailReceiver {
    constructor(s3, s3bucketName, dynamodbDocumentClient) {
        this.s3 = s3;
        this.s3bucketName = s3bucketName;
        this.dynamodbDocumentClient = dynamodbDocumentClient;
    }

    handleEmailNotification(sesNotification) {
        return this.s3GetObjectAsync(sesNotification.mail.messageId)
            .then(EmailReceiver.extractRawEmailBufferFromS3Object)
            .then(EmailReceiver.parseEmailFromRawBufferAsync)
            .then(EmailReceiver.extractCsvAttachmentBufferFromEmail)
            .then(EmailReceiver.translateCsvBufferToJsonObjectsAsync)
            .then((objects) => EmailReceiver.injectConstantProperties(
                {LastIngestedDateTime: sesNotification.mail.timestamp}, objects))
            .then(EmailReceiver.translateObjectsToDynamoPutRequests)
            .then(this.dynamoPutManyAsync.bind(this));
    }

    s3GetObjectAsync(objectKey) {
        return this.s3.getObject({
            Bucket: this.s3bucketName,
            Key: objectKey
        }).promise();
    }

    static extractRawEmailBufferFromS3Object(s3Object) {
        console.log('extractRawEmailBufferFromS3Object');
        // The s3Object's "Body" refers to the body of the HTTP response from S3, not an email body
        return s3Object.Body;
    }

    static parseEmailFromRawBufferAsync(rawEmailBuffer) {
        console.log(`parseEmailFromRawBufferAsync (email buffer length: ${rawEmailBuffer.length})`);
        return mailparser.simpleParser(rawEmailBuffer)
    }

    static extractCsvAttachmentBufferFromEmail(parsedEmail) {
        return stripBomBuf(parsedEmail.attachments[0].content);
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
            .replace(/Sub-type/g, "SubType")
            .replace(/ID/g, "Id")
            .replace(/Date$/g, "DateTime")
            .replace(/Expires/g, "ExpiresDateTime");
    }

    static sanitizePropertyValue(originalValue, propertyName) {
        // DynamoDB doesn't allow empty string attributes - they can either be dropped
        // or replaced with a placeholder. We arbitrarily choose the former.
        if (originalValue === '') {
            return null;
        }

        const dateTimePropertyNameRegex = /(DateTime|DateOfBirth)$/;
        if (dateTimePropertyNameRegex.test(propertyName)) {
            return EmailReceiver.sanitizeDateTime(originalValue);
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

    static translateObjectsToDynamoPutRequests(objects) {
        return objects.map(o => {
            if (typeof o.LastIngestedDateTime !== 'string' || o.LastIngestedDateTime.length === 0) {
                throw new Error("Logic error: attempting to PUT object without valid LastIngestedDateTime: " + JSON.stringify(o, null, 2));
            }
            
            return {
                TableName: TABLE_NAME,
                Item: o,
                ConditionExpression: 'attribute_not_exists(LastIngestedDateTime) or (LastIngestedDateTime < :NewIngestedDateTime)',
                ExpressionAttributeValues: { ':NewIngestedDateTime': o.LastIngestedDateTime },
                ReturnConsumedCapacity: "TOTAL",
                ReturnValues: "NONE"
            };
        });
    }

    // Note that we intentionally avoid calling dynamo's batchWrite APIs, because
    // ~300 concurrent writes is fine for our needs and avoiding batchWrite lets us
    // use the AWS SDK's retry/backoff logic instead of implementing our own.
    dynamoPutManyAsync(requestParams) {
        console.log(`dynamoWriteRequestsAsync (${requestParams.length} requests)`);
        return Promise.all(requestParams.map(this.dynamoPutAsync.bind(this)))
    }

    dynamoPutAsync(singleRequestParam) {
        return this.dynamodbDocumentClient
            .put(singleRequestParam).promise()
            .catch(err => handleDynamoPutError(singleRequestParam, err));
    }

    static handleDynamoPutError(triggeringRequest, err) {
        Object.assign(err, { triggeringDynamoRequest: triggeringRequest });
        if (err.code == 'ConditionalCheckFailedException') {
            // We eat these because in this case, we don't want to prevent the other put requests from triggering
            console.warn('Ignoring ConditionalCheckFailedException on AnimalID ' + triggeringRequest.Item.AnimalId);
            console.warn(JSON.stringify(err, null, 2));
        } else {
            throw err;
        }
    }
}

module.exports = EmailReceiver;