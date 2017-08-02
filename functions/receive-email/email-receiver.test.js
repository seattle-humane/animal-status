'use strict';

const fs = require('fs');
const path = require('path');

const EmailReceiver = require('./email-receiver')
const emailReceiver = new EmailReceiver(null, 'nonexistent-test-bucket');

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

test('extractCsvBufferFromRawEmailBuffer correctly translates from email content to csv', () => {
    var exampleEmailBuffer = fs.readFileSync(path.join(__dirname, 'email-receiver.test.example-email-1-raw.txt'));
    var expectedCsvBuffer = fs.readFileSync(path.join(__dirname, 'email-receiver.test.example-email-1-attachment.csv'));
    
    return expect(emailReceiver.extractCsvBufferFromRawEmailBuffer(exampleEmailBuffer))
        .resolves.toEqual(expectedCsvBuffer);
})

test('santizeColumnName translates "Animal #" to "AnimalId"', () => {
    expect(emailReceiver.sanitizeColumnName('Animal #')).toBe('AnimalId');
});

test('santizeColumnName removes / characters', () => {
    expect(emailReceiver.sanitizeColumnName('Foo/Bar')).toBe('FooBar');
});