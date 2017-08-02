# Manual Setup Steps

This file documents the assorted AWS things I registered to get this all to work that aren't covered by the CloudFormation configurations in resource_configs. Notably, account setup, domain setup, and SES setup are not available in CloudFormation, so they're all manual.

## 1. Create new email address for AWS root account
Register a new gmail address to act as the username for the AWS root account.

Compromising this account will compromise the AWS root account, so **do not reuse an existing email address**.

## 2. Create new AWS root account
Use the new gmail address to create a new Amazon Web Services account.

Compromising this account will compromise all resources, so **use a strong password, 

## 3. Secure the AWS root account
* Follow the recommendations on the [AWS IAM dashboard](https://console.aws.amazon.com/iam): Enable MFA, create a non-root user to authenticate future CLI commands with, etc.
* Secure the email address from step 1 similarly (compromising it compromises the AWS root account): use strong password, use MFA.

## 4. Use AWS Route53 to register a new domain to receive emails at
You can't set up email receiving without a domain you control DNS records for.

It's easiest to set up a separate domain under the same AWS account the rest of your resources will be under.

*You'll need to wait for registration to be complete (you'll get an email) before you can continue to step 5.*

## 5. Verify domain ownership with Simple Email Service
Follow [this guide](http://docs.aws.amazon.com/ses/latest/DeveloperGuide/receiving-email-getting-started-verify.html).

## 6. Add an IP filter for received emails
In the [SES Dashboard](https://console.aws.amazon.com/ses/) at Email Receiving -> IP Address Filters, add an Allow filter corresponding to the source that will be emailing your data exports.

## 7. Set up the CloudFoundation-automatable resources
```serverless deploy```

## 8. Add a Receipt Rule for receive emails
* **Recipients**: Any single arbitrary recipient address
* **Actions**:
  1. S3: S3 Bucket: animal-status-email, no Object key prefix, no Encrypt Message, no SNS topic
  2. Lambda: Lambda function: animal-status-dev-receiveemail, Invocation type Event, no SNS topic
* Everything else default
* Allow AWS to set up the required permissions automatically