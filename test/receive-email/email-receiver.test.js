'use strict';

const fs = require('fs');
const path = require('path');

const emailReceiver = require('../../functions/receive-email/email-receiver')

test('extractRawEmailBufferFromS3Object extracts Body', () => {
    var sampleBodyBuffer = Buffer.alloc(20, "1");
    var sampleS3Object = { Body: sampleBodyBuffer };

    expect(emailReceiver.extractRawEmailBufferFromS3Object(sampleS3Object))
        .toEqual(sampleBodyBuffer);
});

test('extractCsvBufferFromRawEmailBufferAsync correctly translates from email content to csv', () => {
    var exampleEmailBuffer = fs.readFileSync(path.join(__dirname, 'email-receiver.test.example-email-1-raw.txt'));
    var expectedCsvBuffer = fs.readFileSync(path.join(__dirname, 'email-receiver.test.example-email-1-attachment.csv'));
    
    return expect(emailReceiver.extractCsvBufferFromRawEmailBufferAsync(exampleEmailBuffer))
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

test('sanitizeDateTimeProperties invokes sanitizeDateTime on the expected values', () => {
    expect(emailReceiver.sanitizeDateTimeProperties([{ CreatedDateTime: '2/4/2010 12:00 AM' }]))
        .toEqual([{ CreatedDateTime: '2010-02-04T08:00:00Z' }]);

    expect(emailReceiver.sanitizeDateTimeProperties([{ DateOfBirth: '2/4/2010 12:00 AM' }]))
        .toEqual([{ DateOfBirth: '2010-02-04T08:00:00Z' }]);
});

test('sanitizeDateTimeProperties does not invoke sanitizeDateTime on non-date values', () => {
    expect(emailReceiver.sanitizeDateTimeProperties([{ SomeNonDateString: 'foo' }]))
        .toEqual([{ SomeNonDateString: 'foo' }]);
});

test('sanitizeEmptyStringValues ignores non-string properties', () => {
    expect(emailReceiver.sanitizeEmptyStringValues([{ nonstring: 1 }]))
        .toEqual([{ nonstring: 1 }]);
});

test('sanitizeEmptyStringValues ignores non-empty string properties', () => {
    expect(emailReceiver.sanitizeEmptyStringValues([{ nonemptystring: '1' }]))
        .toEqual([{ nonemptystring: '1' }]);
});

test('sanitizeEmptyStringValues removes empty string properties', () => {
    expect(emailReceiver.sanitizeEmptyStringValues([{ emptystring: '' }]))
        .toEqual([{ }]);
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
            }
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