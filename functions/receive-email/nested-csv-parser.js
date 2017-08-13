/*
  Given a Buffer or string containing the contents of a CSV file of the following form:

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
  ]
*/

const csv = require('csv-parser');
const stream = require('stream');

function stringToStream(str) {
    var readableStream = new stream.Readable;
    readableStream.push(str);
    readableStream.push(null); // acts as fake EOF
    return readableStream;
}

class NestedCsvParser {
    static subTypeToPropertyMap(headers, mapHeadersFn) {
        var map = new Object();
        for (var headerIndex in headers) {
            var header = mapHeadersFn(headers[headerIndex]);
            if (header === '__RowSubType') { continue; }

            var splitHeader = header.split(':');
            if (splitHeader.length > 2) {
                throw new Error(`Can't parse header ${header}, contains multiple ':'s`);
            }

            var subType = (splitHeader.length === 1) ? '' : splitHeader[0];
            var propertyName = (subType === '') ? splitHeader[0] : splitHeader[1];

            if (!map.hasOwnProperty(subType)) {
                map[subType] = [];
            }
            map[subType].push(propertyName);
        }
        return map;
    }

    static parseAsync(csvBufferOrString, csvParserConfig = {}) {
        return new Promise(function(resolve, reject) {
            try {
                var mapValue = csvParserConfig.mapValue || (x => x);
                
                var inputStream = stringToStream(csvBufferOrString);

                var subTypeToPropertyMap = null;
                var allObjects = [];
                var currentObject = null;
                
                var innerCsvParser = csv(csvParserConfig);
                inputStream
                    .pipe(innerCsvParser)
                    .on('headers', function(headers) {
                        if (!headers.includes('__RowSubType')) {
                            throw new Error('Invalid CSV headers - must have a __RowSubType column to be parsed as a nested CSV. Instead, got headers: ' + JSON.stringify(headers));
                        }
                        subTypeToPropertyMap = NestedCsvParser.subTypeToPropertyMap(headers, innerCsvParser.mapHeaders);
                    })
                    .on('data', function (data) {
                        var subType = data.__RowSubType;
                        var properties = subTypeToPropertyMap[subType];
                        if (subType === '') {
                            currentObject = new Object();
                            for(var i in properties) {
                                var originalValue = data[properties[i]];
                                var mappedValue = mapValue(originalValue, properties[i]);
                                if (mappedValue !== null) {
                                    currentObject[properties[i]] = mappedValue;
                                }
                            }
                            allObjects.push(currentObject);
                        } else {
                            var subTypePrefix = subType + ':';
                            var subTypeObject = new Object();

                            if(!currentObject.hasOwnProperty(subType)) {
                                currentObject[subType] = [subTypeObject];
                            } else {
                                currentObject[subType].push(subTypeObject);
                            }
                            
                            for(var i in properties) {
                                var originalValue = data[subTypePrefix + properties[i]];
                                var mappedValue = mapValue(originalValue, properties[i]);
                                if (mappedValue !== null) {
                                    subTypeObject[properties[i]] = mappedValue;
                                }
                            }
                        }
                    })
                    .on('end', function () {
                        resolve(allObjects);
                    })
            } catch(e) { reject(e); }
        });
    }
}

module.exports = NestedCsvParser;