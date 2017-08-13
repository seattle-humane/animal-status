'use strict';

const awssdk = require('aws-sdk');
const s3 = new awssdk.S3()
const s3BucketName = 'animal-status-email';
const dynamoDocumentClient = new awssdk.DynamoDB.DocumentClient();

const EmailReceiver = require('./email-receiver')
const emailReceiver = new EmailReceiver(s3, s3BucketName, dynamoDocumentClient)

exports.handler = function (event, context, lambda_callback) {
    var sesNotification = event.Records[0].ses;
    console.log("SES Notification:\n", JSON.stringify(sesNotification, null, 2));

    emailReceiver.handleEmailNotification(sesNotification)
        .then(() => lambda_callback(null, null))
        .catch(lambda_callback);
}
