'use strict';

const fs = require('fs');
const path = require('path');

const emailReceiver = require('../../functions/receive-email/email-receiver');

function awsStylePromiseContainerAround(data) {
    return {
        promise: () => new Promise((resolve, reject) => { resolve(data); })
    };
}

test('example1.single-dog works end-to-end with AWS calls mocked out', () => {
    var example1SesNotification = JSON.parse(fs.readFileSync(path.join(__dirname, 'example1.single-dog.sesNotification.json')));
    var example1S3MessageContent = fs.readFileSync(path.join(__dirname, 'example1.single-dog.s3MessageContent.blob'));
    var example1S3Object = { Body: example1S3MessageContent };
    var example1ExpectedDynamoRequest = JSON.parse(fs.readFileSync(path.join(__dirname, 'example1.single-dog.dynamoRequest.json')));

    var mockS3BucketName = 'SomeS3BucketName';
    var messageIdFromSesNotification = 'i8zg02md8jbqrlqv4vtf8vorr8tlkaak6439bao1';
    var mockDynamoPutResponse = { TableName: 'Animals' };

    var mockS3 = { getObject: jest.fn() };
    mockS3.getObject.mockReturnValue(awsStylePromiseContainerAround(example1S3Object));

    var mockDynamoClient = { put: jest.fn() };
    mockDynamoClient.put.mockReturnValue(awsStylePromiseContainerAround(mockDynamoPutResponse));

    var receiver = new emailReceiver(mockS3, mockS3BucketName, mockDynamoClient);

    return receiver.handleEmailNotification(example1SesNotification).then(() => {
        expect(mockS3.getObject.mock.calls.length).toBe(1);
        expect(mockS3.getObject.mock.calls[0][0]).toEqual({
            Bucket: mockS3BucketName,
            Key: messageIdFromSesNotification
        });

        expect(mockDynamoClient.put.mock.calls.length).toBe(1);
        expect(mockDynamoClient.put.mock.calls[0][0]).toEqual(example1ExpectedDynamoRequest);
    });
});

test('extractRawEmailBufferFromS3Object extracts Body', () => {
    var sampleBodyBuffer = Buffer.alloc(20, "1");
    var sampleS3Object = { Body: sampleBodyBuffer };

    expect(emailReceiver.extractRawEmailBufferFromS3Object(sampleS3Object))
        .toEqual(sampleBodyBuffer);
});

test('extractCsvBufferFromRawEmailBufferAsync correctly translates from email content to csv', () => {
    var exampleEmailBuffer = fs.readFileSync(path.join(__dirname, 'example1.single-dog.s3MessageContent.blob'));
    var expectedCsvBuffer = fs.readFileSync(path.join(__dirname, 'example1.single-dog.csv'));
    
    return expect(emailReceiver.parseEmailFromRawBufferAsync(exampleEmailBuffer)
        .then(emailReceiver.extractCsvAttachmentBufferFromEmail))
        .resolves.toEqual(expectedCsvBuffer);
});

test('translateCsvBufferToJsonObjects calls sanitizeColumnName', () => {
    const inputCsvBuffer = Buffer.from(
`__RowSubType,Animal #
,A123`);
    return expect(emailReceiver.translateCsvBufferToJsonObjectsAsync(inputCsvBuffer))
        .resolves.toEqual([{AnimalId: 'A123'}]);
});

test('sanitizeColumnName sanitizes real example inputs as expected', () => {
    expect(emailReceiver.sanitizeColumnName('Animal #')).toEqual('AnimalId');
    expect(emailReceiver.sanitizeColumnName('Last Updated Date/Time')).toEqual('LastUpdatedDateTime');
    expect(emailReceiver.sanitizeColumnName('Created Date/Time')).toEqual('CreatedDateTime');
    expect(emailReceiver.sanitizeColumnName('Review Date')).toEqual('ReviewDateTime');
    expect(emailReceiver.sanitizeColumnName('Memo By (login)')).toEqual('MemoBy');
    expect(emailReceiver.sanitizeColumnName('Pet ID Number')).toEqual('PetIdNumber');
    expect(emailReceiver.sanitizeColumnName('Sub-location')).toEqual('SubLocation');
    expect(emailReceiver.sanitizeColumnName('Date of Birth')).toEqual('DateOfBirth');
    expect(emailReceiver.sanitizeColumnName('Expires')).toEqual('ExpiresDateTime');
    expect(emailReceiver.sanitizeColumnName('Sub-type')).toEqual('SubType');
});

test('sanitizeDateTime converts from original format to ISO8601', () => {
    expect(emailReceiver.sanitizeDateTime('2/4/2010 12:00 AM')).toEqual('2010-02-04T08:00:00Z');
});

test('sanitizeDateTime preserves minutes', () => {
    expect(emailReceiver.sanitizeDateTime('8/12/2017 11:56 AM')).toMatch(/:56:00Z$/);
});

test('injectConstantProperties merges constant properties into input objects', () => {
    expect(emailReceiver.injectConstantProperties(
        { foo: 'foo', bar: 'bar' },
        [{ id: 1 }, { id: 2 }]))
        .toEqual([
            { id: 1, foo: 'foo', bar: 'bar' },
            { id: 2, foo: 'foo', bar: 'bar' },
        ]);
});

function SomeDynamoCapableObject() { return { LastIngestedDateTime: "2017-08-13T01:58:56.622Z" } }

test('translateObjectsToDynamoPutRequests creates one request item per input object', () => {
    expect(
        emailReceiver.translateObjectsToDynamoPutRequests(
            [SomeDynamoCapableObject(), SomeDynamoCapableObject(), SomeDynamoCapableObject()]))
        .toHaveLength(3);
});

test('translateObjectsToDynamoPutRequests fails informatively if input object is missing required property LastIngestedDateTime', () => {
    expect(
        () => emailReceiver.translateObjectsToDynamoPutRequests([ { } ]))
        .toThrowError(/LastIngestedDateTime/);
});

test('translateObjectsToDynamoPutRequests translates objects to correct conditional PUT request', () => {
    expect(
        emailReceiver.translateObjectsToDynamoPutRequests(
            [{ id: 1, LastIngestedDateTime: '2017-08-13T01:58:56.622Z' }]))
        .toEqual([
            {
                TableName: 'Animals',
                Item: { id: 1, LastIngestedDateTime: '2017-08-13T01:58:56.622Z' },
                ConditionExpression: 'attribute_not_exists(LastIngestedDateTime) or (LastIngestedDateTime < :NewIngestedDateTime)',
                ExpressionAttributeValues: { ':NewIngestedDateTime': '2017-08-13T01:58:56.622Z' },
                ReturnConsumedCapacity: "TOTAL",
                ReturnValues: "NONE"
            }]);
});
