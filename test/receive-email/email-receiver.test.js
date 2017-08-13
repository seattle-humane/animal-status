'use strict';

const fs = require('fs');
const path = require('path');

const emailReceiver = require('../../functions/receive-email/email-receiver')

test('inferTableNameFromEmailSubject infers Animals table for corresponding subject', () => {
    expect(emailReceiver.inferTableNameFromEmailSubject(
        'Report animal-status data export - Animals has been completed.'
    )).toBe('Animals');
});

test('inferTableNameFromEmailSubject infers AnimalMemos table for corresponding subject', () => {
    expect(emailReceiver.inferTableNameFromEmailSubject(
        'Report animal-status data export - AnimalMemos has been completed.'
    )).toBe('AnimalMemos');
});

test('inferTableNameFromEmailSubject infers AnimalHolds table for corresponding subject', () => {
    expect(emailReceiver.inferTableNameFromEmailSubject(
        'Report animal-status data export - AnimalHolds has been completed.'
    )).toBe('AnimalHolds');
});

test('inferTableNameFromEmailSubject infers AnimalBehaviorTests table for corresponding subject', () => {
    expect(emailReceiver.inferTableNameFromEmailSubject(
        'Report animal-status data export - AnimalBehaviorTests has been completed.'
    )).toBe('AnimalBehaviorTests');
});

test('inferTableNameFromEmailSubject infers AnimalPetIds table for corresponding subject', () => {
    expect(emailReceiver.inferTableNameFromEmailSubject(
        'Report animal-status data export - AnimalPetIds has been completed.'
    )).toBe('AnimalPetIds');
});

test('inferTableNameFromEmailSubject throws an Error for an unrecognized subject', () => {
    expect(() => emailReceiver.inferTableNameFromEmailSubject(
        'Report animal-status data export - GarbageTableName has been completed.'
    )).toThrow();
});

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

test('santizeColumnName translates "Animal #" to "AnimalId"', () => {
    expect(emailReceiver.sanitizeColumnName('Animal #')).toBe('AnimalId');
});

test('santizeColumnName removes / characters', () => {
    expect(emailReceiver.sanitizeColumnName('Foo/Bar')).toBe('FooBar');
});

test('translateCsvBufferToJsonObjects translates the simplest possible CSV', () => {
    const inputCsvBuffer = Buffer.from('"Key"\n"Value"');
    return expect(emailReceiver.translateCsvBufferToJsonObjectsAsync(inputCsvBuffer))
        .resolves.toEqual([{Key: 'Value'}]);
});

test('translateCsvBufferToJsonObjects calls sanitizeColumnName', () => {
    const inputCsvBuffer = Buffer.from('"Animal #"\n"A123"');
    return expect(emailReceiver.translateCsvBufferToJsonObjectsAsync(inputCsvBuffer))
        .resolves.toEqual([{AnimalId: 'A123'}]);
});

test('sanitizeColumnName sanitizes real example inputs as expected', () => {
    expect(emailReceiver.sanitizeColumnName('Animal #')).toEqual('AnimalId');
    expect(emailReceiver.sanitizeColumnName('Last Updated Date/Time')).toEqual('LastUpdatedDateTime');
    expect(emailReceiver.sanitizeColumnName('Review Date')).toEqual('ReviewDateTime');
    expect(emailReceiver.sanitizeColumnName('Memo By (login)')).toEqual('MemoBy');
    expect(emailReceiver.sanitizeColumnName('Pet ID Number')).toEqual('PetIdNumber');
    expect(emailReceiver.sanitizeColumnName('Sub-location')).toEqual('SubLocation');
});

test('sanitizeDateTime converts from original format to ISO8601', () => {
    expect(emailReceiver.sanitizeDateTime('2/4/2010 12:00 AM')).toEqual('2010-02-04T08:00:00Z');
});

test('sanitizeDateTimeProperties invokes sanitizeDateTime on the expected values', () => {
    expect(emailReceiver.sanitizeDateTimeProperties([{ CreatedDateTime: '2/4/2010 12:00 AM' }]))
        .toEqual([{ CreatedDateTime: '2010-02-04T08:00:00Z' }]);
});

test('injectDerivedObjectProperties injects a BehaviorCategory-BehaviorTest property', () => {
    expect(emailReceiver.injectDerivedObjectProperties([{BehaviorCategory: 'foo', BehaviorTest: 'bar'}]))
        .toEqual([{ BehaviorCategory: 'foo', BehaviorTest: 'bar', 'BehaviorCategory-BehaviorTest':'foo-bar' }]);
});

test('injectDerivedObjectProperties noops for objects that dont need derived properties', () => {
    expect(emailReceiver.injectDerivedObjectProperties([{ Foo: 'foo', Bar: 'bar' }]))
        .toEqual([{ Foo: 'foo', Bar: 'bar'}]);
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
        emailReceiver.translateObjectsToDynamoRequests('TestTable',
            Array.from(new Array(25), SomeDynamoCapableObject)))
        .toHaveLength(1);
        
    expect(
        emailReceiver.translateObjectsToDynamoRequests('TestTable',
            Array.from(new Array(26), SomeDynamoCapableObject)))
        .toHaveLength(2);
    
    expect(
        emailReceiver.translateObjectsToDynamoRequests('TestTable',
            Array.from(new Array(51), SomeDynamoCapableObject)))
        .toHaveLength(3);
});

test('translateObjectsToDynamoRequests creates one PUT request item per input object', () => {
    expect(
        emailReceiver.translateObjectsToDynamoRequests('TestTable',
            [SomeDynamoCapableObject(), SomeDynamoCapableObject(), SomeDynamoCapableObject()])
            [0].RequestItems['TestTable'])
        .toHaveLength(3);
});

test('translateObjectsToDynamoRequests fails informatively if input object is missing required property LastIngestedDateTime', () => {
    expect(
        () => emailReceiver.translateObjectsToDynamoRequests('TestTable', [ { } ]))
        .toThrowError(/LastIngestedDateTime/);
});

test('translateObjectsToDynamoRequests translates objects to correct conditional PutRequest', () => {
    expect(
        emailReceiver.translateObjectsToDynamoRequests('TestTable',
            [{ id: 1, LastIngestedDateTime: '2017-08-13T01:58:56.622Z' }]))
        .toEqual([{
            RequestItems: {
                'TestTable': [
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