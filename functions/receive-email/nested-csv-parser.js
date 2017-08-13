/*
  Given a Buffer containing the contents of a CSV file of the following form:

  __RowSubType, IdProperty   , BaseProperty1, SubType1:P1, SubType1:P2, SubType2:P1, SubType2:P2
              , 1            , BP1_A        ,            ,            ,            ,            
  SubType1    , 1            ,              , ST1P1_A    , ST1P2_A    ,            ,
  SubType1    , 1            ,              , ST1P1_B    , ST1P2_B    ,            ,
  SubType2    , 1            ,              ,            ,            , ST2P1_A    , ST2P2_A
  SubType2    , 1            ,              ,            ,            , ST2P1_B    , ST2P2_B
              , 2            , BP1_C        ,            ,            ,            ,
  SubType2    , 2            ,              ,            ,            , ST2P1_C    , ST2P2_C

  ... outputs JSON of the form:

  [
      {
          IdProperty: 1,
          BaseProperty1: 'BP1_A',
          SubType1: [
              {
                  P1: 'ST1P1_A',
                  P2: 'ST1P2:A'
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
          IdProperty: 2,
          BaseProperty1: 'BP1_C',
          SubType2: [
              {
                  P1: 'ST2P1_C',
                  P2: 'ST2P2_C'
              }
          ]
      }
  ]
*/

const csvparse = require('csv-parse');



module.exports = function (csvBuffer) {
    const parseOptions = { columns: EmailReceiver.sanitizeColumnNames }

    return new Promise(function (resolve, reject) {
        return csvparse(csvBuffer, parseOptions, function (err, output) {
            if (err) { reject(err); }
            resolve(output);
        });
    });
}