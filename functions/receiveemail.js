const awssdk = require('aws-sdk');
const mailparser = require('mailparser');
const csvparse = require('csv-parse');

const bucketName = 'animal-status-email';

exports.handler = function (event, context, lambda_callback) {
    var sesNotification = event.Records[0].ses;
    console.log("SES Notification:\n", JSON.stringify(sesNotification, null, 2));

    var tableName = inferTableNameFromEmailSubject(sesNotification.mail.commonHeaders.subject);
    console.log("Inferred table name " + tableName + " from email subject");

    // Retrieve the email from your bucket
    awssdk.S3().getObject({
        Bucket: bucketName,
        Key: sesNotification.mail.messageId
    }).promise()
        .then(extractRawEmailBufferFromS3Object)
        .then(extractCsvAttachmentFromRawEmailBuffer)
        /*.then(sanitizeCsvHeaders)
        .then(translateCsvRowsToJsonObjects)
        .then(bind(insertObjectsIntoDynamoTable, tableName))*/
        .then(_ => lambda_callback(null, null))
        .catch(lambda_callback);
};

function inferTableNameFromEmailSubject(subject) {
    if (subject.search(/Memo/)) {
        return "AnimalMemos";
    } else if(subject.search(/Hold/)) {
        return "AnimalHolds";
    } else if(subject.search(/Behavior/)) {
        return "AnimalBehaviorTests";
    } else if(subject.search(/PetID/i)) {
        return "AnimalPetIds";
    } else if(subject.search(/Animal/)) {
        // Important that this is last
        return "Animals";
    } else {
        throw new Error("Could not infer table name from subject " + subject)
    }
}

function extractRawEmailBufferFromS3Object(s3Object) {
    // The s3Object's "Body" refers to the body of the HTTP response from S3, not an email body
    return s3Object.Body.data;
}

function extractCsvAttachmentFromRawEmailBuffer(rawEmailBuffer) {
    return mailparser.simpleParser(rawEmailBuffer)
        .then(mail => console.log("mailparser got attachment: " + mail.attachments[0].content))
}
