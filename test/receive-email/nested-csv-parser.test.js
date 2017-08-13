'use strict';

const nestedCsvParser = require('../../functions/receive-email/nested-csv-parser');

test('Example from documentation works end to end', () => {
    var documentedInput = 
`__RowSubType, IdProperty   , BaseProperty1, SubType1:P1, SubType1:P2, SubType2:P1, SubType2:P2
            , 1            , BP1_A        ,            ,            ,            ,            
SubType1    ,              ,              , ST1P1_A    , ST1P2_A    ,            ,
SubType1    ,              ,              , ST1P1_B    , ST1P2_B    ,            ,
SubType2    ,              ,              ,            ,            , ST2P1_A    , ST2P2_A
SubType2    ,              ,              ,            ,            , ST2P1_B    , ST2P2_B
            , 2            , BP1_C        ,            ,            ,            ,
SubType2    ,              ,              ,            ,            , ST2P1_C    , ST2P2_C`
        .replace(/ /g,'');

    var documentedOutput = [
        {
            IdProperty: '1',
            BaseProperty1: 'BP1_A',
            SubType1: [
                {
                    P1: 'ST1P1_A',
                    P2: 'ST1P2_A'
                },
                {
                    P1: 'ST1P1_B',
                    P2: 'ST1P2_B'
                }
            ],
            SubType2: [
                {
                    P1: 'ST2P1_A',
                    P2: 'ST2P2_A'
                },
                {
                    P1: 'ST2P1_B',
                    P2: 'ST2P2_B'
                }
            ]
        },
        {
            IdProperty: '2',
            BaseProperty1: 'BP1_C',
            SubType2: [
                {
                    P1: 'ST2P1_C',
                    P2: 'ST2P2_C'
                }
            ]
        }
    ];

    return expect(nestedCsvParser.parseAsync(documentedInput))
        .resolves.toEqual(documentedOutput);
});

test('subTypeToPropertyMap correctly groups example headers', () => {
    expect(nestedCsvParser.subTypeToPropertyMap([
        '__RowSubType',
        'BaseProperty1',
        'BaseProperty2',
        'SubType1:SubType1Property1',
        'SubType1:SubType1Property2',
        'SubType2:SubType2Property1'], x => x))
        .toEqual({
            '': ['BaseProperty1', 'BaseProperty2'],
            'SubType1': ['SubType1Property1', 'SubType1Property2'],
            'SubType2': ['SubType2Property1']
        });
});

test('parseAsync handles quoted multiline values correctly', () => {
    var inputCsv =
`__RowSubType,BaseProperty,SubType:SubTypeProperty
,"Some multi-line
text in BaseProperty",
SubType,,"Some more multi-line
text in SubTypeProperty"`;

    var expectedOutput = [{BaseProperty:`Some multi-line
text in BaseProperty`, SubType: [{SubTypeProperty: `Some more multi-line
text in SubTypeProperty`}]}];

    return expect(nestedCsvParser.parseAsync(inputCsv))
        .resolves.toEqual(expectedOutput);
});

test('parseAsync translates headers with mapHeaders config property', () => {
    var inputCsv =
`__RowSubType,BaseFoo,SubType:SubTypeFoo
,x,
SubType,,x`;

    var fooToBarHeaderMapper = (header => header.replace('Foo', 'Bar'))

    var expectedOutput = [{
        BaseBar: 'x',
        SubType: [{
            SubTypeBar: 'x'
        }] 
    }];

    return expect(nestedCsvParser.parseAsync(inputCsv, { mapHeaders: fooToBarHeaderMapper}))
        .resolves.toEqual(expectedOutput);
});

test('parseAsync translates values with mapValue config property', () => {
    var inputCsv =
        `__RowSubType,BaseProperty,SubType:SubTypeProperty
,Foo,
SubType,,Foo`;

    var fooToBarValueMapper = (value => value.replace('Foo', 'Bar'))

    var expectedOutput = [{
        BaseProperty: 'Bar',
        SubType: [{
            SubTypeProperty: 'Bar'
        }]
    }];

    return expect(nestedCsvParser.parseAsync(inputCsv, { mapValue: fooToBarValueMapper }))
        .resolves.toEqual(expectedOutput);
});

test('parseAsync provides mapValue with property names that have already passed through mapHeaders', () => {
    var inputCsv =
`__RowSubType,BaseFoo,SubType:SubTypeFoo
,x,
SubType,,x`;

    var fooToBarHeaderMapper = (header => header.replace('Foo', 'Bar'))
    var appendHeaderNameValueMapper = ((value, header) => (value + header));

    var expectedOutput = [{
        BaseBar: 'xBaseBar', // not xBaseFoo
        SubType: [{
            SubTypeBar: 'xSubTypeBar' // not xSubTypeFoo
        }] 
    }];

    return expect(nestedCsvParser.parseAsync(inputCsv, {
        mapHeaders: fooToBarHeaderMapper,
        mapValue: appendHeaderNameValueMapper
    })).resolves.toEqual(expectedOutput);
});

test('parseAsync omits values that mapValue returns null for', () => {
    var inputCsv =
        `__RowSubType,BaseProperty1,BaseProperty2,SubType:SubTypeProperty1,SubType:SubTypeProperty2
,Foo,Bar,,
SubType,,,Foo,Baz`;

    var fooToNullValueMapper = (value => (value === 'Foo') ? null : value);

    var expectedOutput = [{
        // BaseProperty1: 'Foo'/null, <-- should be omitted
        BaseProperty2: 'Bar',
        SubType: [{
            // SubTypeProperty1: 'Foo'/null <-- should be omitted
            SubTypeProperty2: 'Baz'
        }]
    }];

    return expect(nestedCsvParser.parseAsync(inputCsv, { mapValue: fooToNullValueMapper }))
        .resolves.toEqual(expectedOutput);
});