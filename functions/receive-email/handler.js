'use strict';

const awssdk = require('aws-sdk');
const s3 = new awssdk.S3()
const bucketName = 'animal-status-email';

const EmailReceiver = require('functions/receive-email/email-receiver')
const emailReceiver = new EmailReceiver(s3, bucketName)

exports.handler = function (event, context, lambda_callback) {
    var sesNotification = event.Records[0].ses;
    console.log("SES Notification:\n", JSON.stringify(sesNotification, null, 2));

    emailReceiver.handleEmailNotification(sesNotification)
        .then(() => lambda_callback(null, null))
        .catch(lambda_callback);
}
