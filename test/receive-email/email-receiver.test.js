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

    var mockS3 = { getObject: jest.fn() };
    mockS3.getObject.mockReturnValue(awsStylePromiseContainerAround(example1S3Object));

    var mockDynamoClient = { batchWrite: jest.fn() };
    mockDynamoClient.batchWrite.mockReturnValue(awsStylePromiseContainerAround(undefined));

    var receiver = new emailReceiver(mockS3, mockS3BucketName, mockDynamoClient);

    return receiver.handleEmailNotification(example1SesNotification).then(() => {
        expect(mockS3.getObject.mock.calls.length).toBe(1);
        expect(mockS3.getObject.mock.calls[0][0]).toEqual({
            Bucket: mockS3BucketName,
            Key: messageIdFromSesNotification
        });

        expect(mockDynamoClient.batchWrite.mock.calls.length).toBe(1);
        expect(mockDynamoClient.batchWrite.mock.calls[0][0]).toEqual(example1ExpectedDynamoRequest);
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

test('translateObjectsToDynamoRequests creates one batchwrite request per 25 input objects', () => {
    expect(
        emailReceiver.translateObjectsToDynamoRequests(
            Array.from(new Array(25), SomeDynamoCapableObject)))
        .toHaveLength(1);
        
    expect(
        emailReceiver.translateObjectsToDynamoRequests(
            Array.from(new Array(26), SomeDynamoCapableObject)))
        .toHaveLength(2);
    
    expect(
        emailReceiver.translateObjectsToDynamoRequests(
            Array.from(new Array(51), SomeDynamoCapableObject)))
        .toHaveLength(3);
});

test('translateObjectsToDynamoRequests creates one PUT request item per input object', () => {
    expect(
        emailReceiver.translateObjectsToDynamoRequests(
            [SomeDynamoCapableObject(), SomeDynamoCapableObject(), SomeDynamoCapableObject()])
            [0].RequestItems['Animals'])
        .toHaveLength(3);
});

test('translateObjectsToDynamoRequests fails informatively if input object is missing required property LastIngestedDateTime', () => {
    expect(
        () => emailReceiver.translateObjectsToDynamoRequests([ { } ]))
        .toThrowError(/LastIngestedDateTime/);
});

test('translateObjectsToDynamoRequests translates objects to correct conditional PutRequest', () => {
    expect(
        emailReceiver.translateObjectsToDynamoRequests(
            [{ id: 1, LastIngestedDateTime: '2017-08-13T01:58:56.622Z' }]))
        .toEqual([{
            RequestItems: {
                'Animals': [
                    {
                        PutRequest: {
                            Item: { id: 1, LastIngestedDateTime: '2017-08-13T01:58:56.622Z' },
                            ConditionExpression: '#OldIngestedDateTime < :NewIngestedDateTime',
                            ExpressionAttributeNames: { '#OldIngestedDateTime': 'LastIngestedDateTime' },
                            ExpressionAttributeValues: { ':NewIngestedDateTime': '2017-08-13T01:58:56.622Z' }
                        }
                    }
                ]
            },
            ReturnConsumedCapacity: "TOTAL"
        }]);
});

test('paginateArray produces one array on input that fits in page size', () => {
    expect(emailReceiver.paginateArray([], 1)).toEqual([]);
    expect(emailReceiver.paginateArray([1, 2, 3], 3)).toEqual([[1, 2, 3]]);
});

test('paginateArray splits arrays that are multiples of page size into that many pages', () => {
    expect(emailReceiver.paginateArray([1, 2], 1)).toEqual([[1], [2]]);
    expect(emailReceiver.paginateArray([1, 2, 3], 1)).toEqual([[1], [2], [3]]);
    expect(emailReceiver.paginateArray([1, 2, 3, 4], 2)).toEqual([[1, 2], [3, 4]]);
});

test('paginateArray splits arrays that are not multiples of page size with one smaller array at end for leftovers', () => {
    expect(emailReceiver.paginateArray([1, 2, 3], 2)).toEqual([[1, 2], [3]]);
    expect(emailReceiver.paginateArray([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
});

test('logDynamoResponse ignores null responses (for ease of other tests)', () => {
    var mockLogger = jest.fn();
    emailReceiver.logDynamoResponse(null, mockLogger);
    expect(mockLogger.mock.calls.length).toEqual(0);
});

test('logDynamoResponse ignores undefined responses (for ease of other tests)', () => {
    var mockLogger = jest.fn();
    emailReceiver.logDynamoResponse(undefined, mockLogger);
    expect(mockLogger.mock.calls.length).toEqual(0);
});

test('logDynamoResponse logs when no animals went unprocessed', () => {
    const sampleResponse = {
        "UnprocessedItems": { },
        "ConsumedCapacity": "irrelevant"
    };

    var mockLogger = jest.fn();
    emailReceiver.logDynamoResponse(sampleResponse, mockLogger);
    expect(mockLogger).toHaveBeenCalledWith('Unprocessed Animal Count: 0');
});

test('logDynamoResponse logs the number of unprocessed animals', () => {
    const sampleResponse = {
        "UnprocessedItems": {
            "Animals": [
                { "PutRequest": { "Item": { "AnimalId": "A34892333" } } },
                { "PutRequest": { "Item": { "AnimalId": "A34892334" } } }
            ]
        },
        "ConsumedCapacity": "irrelevant"
    };

    var mockLogger = jest.fn();
    emailReceiver.logDynamoResponse(sampleResponse, mockLogger);
    expect(mockLogger).toHaveBeenCalledWith('Unprocessed Animal Count: 2');
});

test('logDynamoResponse logs consumed capacity information', () => {
    const sampleResponse = {
        "UnprocessedItems": { },
        "ConsumedCapacity": [
            {
                "TableName": "Animals",
                "CapacityUnits": 42
            }
        ]
    };

    var mockLogger = jest.fn();
    emailReceiver.logDynamoResponse(sampleResponse, mockLogger);

    expect(mockLogger).toHaveBeenCalledWith('ConsumedCapacity: [{"TableName":"Animals","CapacityUnits":42}]');
});

test('logDynamoResponse logs AnimalIds of UnprocessedItems', () => {
    const sampleResponse = {
        "UnprocessedItems": {
            "Animals": [
                {
                    "PutRequest": {
                        "Item": {
                            "IrrelevantField": "foo",
                            "AnimalId": "A34892332"
                        }
                    }
                },
                {
                    "PutRequest": {
                        "Item": {
                            "IrrelevantField": "bar",
                            "AnimalId": "A34892333"
                        }
                    }
                }
            ]
        },
        "ConsumedCapacity": "irrelevant"
    };

    var mockLogger = jest.fn();
    emailReceiver.logDynamoResponse(sampleResponse, mockLogger);
    expect(mockLogger).toHaveBeenCalledWith('Unprocessed AnimalIds: ["A34892332","A34892333"]');
});