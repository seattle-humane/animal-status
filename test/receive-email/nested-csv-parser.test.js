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
        'SubType2:SubType2Property1']))
        .toEqual({
            '': ['BaseProperty1', 'BaseProperty2'],
            'SubType1': ['SubType1Property1', 'SubType1Property2'],
            'SubType2': ['SubType2Property1']
        });
});