'use strict';

const mailparser = require('mailparser');
const csvparse = require('csv-parse/lib/sync')

class EmailReceiver {
    constructor(s3, bucketName) {
        this.s3 = s3;
        this.bucketName = bucketName;
    }

    handleEmailNotification(sesNotification) {
        var tableName = this.inferTableNameFromEmailSubject(sesNotification.mail.commonHeaders.subject);
        console.log("Inferred table name " + tableName + " from email subject");

        // Retrieve the email from your bucket
        return this.s3.getObject({
            Bucket: this.bucketName,
            Key: sesNotification.mail.messageId
        }).promise()
            .then(this.extractRawEmailBufferFromS3Object)
            .then(this.extractCsvBufferFromRawEmailBuffer)
            .then(this.translateCsvBufferToJsonObjects)
            .then(this.logObject);
            /*.then(bind(insertObjectsIntoDynamoTable, tableName))*/
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

    extractCsvBufferFromRawEmailBuffer(rawEmailBuffer) {
        return mailparser.simpleParser(rawEmailBuffer)
            .then(mail => mail.attachments[0].content);
    }

    translateCsvBufferToJsonObjects(csvBuffer) {
        return csvparse(csvBuffer, { columns: this.sanitizeColumnNames });
    }

    sanitizeColumnNames(columnNames) {
        return columnNames.map(this.sanitizeColumnName);
    }

    sanitizeColumnName(columnName) {
        return columnName
            .replace("#", "Id")
            .replace(" ", "")
            .replace("/", "");
    }
}

module.exports = EmailReceiver;