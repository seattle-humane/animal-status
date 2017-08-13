'use strict';

const fs = require('fs');
const path = require('path');

const EmailReceiver = require('../../functions/receive-email/email-receiver')
const emailReceiver = new EmailReceiver(null, 'nonexistent-test-bucket', null);

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

function SomeDynamoCapableObject() { return { LastInjestedDateTime: "" } }

test('translateObjectsToDynamoRequest creates one request item per input object', () => {
    expect(
        emailReceiver.translateObjectsToDynamoRequest('TestTable',
            [SomeDynamoCapableObject(), SomeDynamoCapableObject(), SomeDynamoCapableObject()])
            .RequestItems['TestTable'])
        .toHaveLength(3);
});

test('translateObjectsToDynamoRequest translates objects to correct conditional PutRequest', () => {
    expect(
        emailReceiver.translateObjectsToDynamoRequest('TestTable',
            [{ id: 1, LastIngestedDateTime: '2010-02-04T08:00:00Z' }]))
        .toEqual({
            RequestItems: {
                'TestTable': [
                    {
                        PutRequest: {
                            Item: { id: 1, LastIngestedDateTime: '2010-02-04T08:00:00Z' },
                            ConditionExpression: '#OldIngestedDateTime < :NewIngestedDateTime',
                            ExpressionAttributeNames: { '#OldIngestedDateTime': 'LastIngestedDateTime' },
                            ExpressionAttributeValues: { ':NewIngestedDateTime': '2010-02-04T08:00:00Z' }
                        }
                    }
                ]
            }
        });
});
