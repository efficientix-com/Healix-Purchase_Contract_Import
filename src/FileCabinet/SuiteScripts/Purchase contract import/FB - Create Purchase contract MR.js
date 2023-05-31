/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @name FB - Create Purchase contract MR
 * @version 1.0
 * @author Dylan Mendoza <dylan.mendoza@freebug.mx>
 * @summary This script will create or update purchase contracts from loaded CSV files.
 * @copyright Tekiio México 2023
 * 
 * Client              -> Healix
 * Last modification   -> 31/05/2023
 * Modified by         -> Dylan Mendoza <dylan.mendoza@freebug.mx>
 * Script in NS        -> FB - Create Purchase contract MR <customscript_fb_carte_purchase_mr>
 */
define(['N/file', 'N/log', 'N/record', 'N/search', 'N/runtime', './moment.js', 'N/config'],
    /**
 * @param{file} file
 * @param{log} log
 * @param{record} record
 * @param{search} search
 * @param{runtime} runtime
 */
    (file, log, record, search, runtime, moment, config) => {
        var TOTALCONTRACTS = 0;
        var CONTRACTPAS = 0;
        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            var parameter_record = runtime.getCurrentScript().getParameter({name: "custscript_fb_carte_record_to_process"});
            log.audit({title:'Inicio de procesamiento', details:parameter_record});
            try {
                updateTrackingRecord(parameter_record, 6, '', '', true);
                // updatePercent(0,0);
                var recordFile = search.lookupFields({
                   type: 'customrecord_fb_uploaded_files',
                   id: parameter_record,
                   columns: ['custrecord_fb_tracking_csv_file']
                });
                recordFile = recordFile.custrecord_fb_tracking_csv_file[0].value;
                log.debug({title:'recordFile', details:recordFile});
                if (recordFile) {
                    var fileContent = file.load({
                        id: recordFile
                    });
                    var contenido_file = fileContent.getContents();
                    var iterator = fileContent.lines.iterator();
                    var dataGroups = {};
                    var newDataGroups = {};
                    var idsContract = [];
                    var newContract = [];
                    var linenumber = 0;
                    iterator.each(function(line){
                        var lineValues = line.value.split(',');
                        // log.debug({title:'lineValues: ' + linenumber, details:lineValues});
                        if (linenumber == 0) {
                            if (lineValues[0].toLowerCase() == 'id' && lineValues[1].toLowerCase() == 'vendor number' 
                            && lineValues[2].toLowerCase() == 'vendor name' && lineValues[3].toLowerCase() == 'subsidiary' 
                            && lineValues[4].toLowerCase() == 'location' && lineValues[5].toLowerCase() == 'date' 
                            && lineValues[6].toLowerCase() == 'item sku' && lineValues[7].toLowerCase() == 'item name' 
                            && lineValues[8].toLowerCase() == 'quantity' && lineValues[9].toLowerCase() == 'rate' 
                            && lineValues[10].toLowerCase() == 'ship to contract' && lineValues[11].toLowerCase() == 'customer contract') {
                                linenumber++;
                                return true;
                            }else{ // No se tiene bien la estructura
                                updateTrackingRecord(parameter_record, 6, 'The file does not have the correct structure.', '', false);
                                return false;
                            }
                        }
                        // log.debug({title:'Validacion', details:idsContract.indexOf(lineValues[0])});
                        if (lineValues[0] != '') { // Linea para agregar a contrato
                            if (idsContract.indexOf(lineValues[0]) == -1) { // no hay grupo para esta linea
                                dataGroups[lineValues[0]] = {lines:[lineValues]};
                                idsContract.push(lineValues[0]);
                            }else{ // ya existe grupo para esta linea
                                dataGroups[lineValues[0]].lines.push(lineValues);
                            }
                        }else{ // nuevos contratos
                            var newContractId = lineValues[10]+'-'+lineValues[1];
                            if (newContract.indexOf(newContractId) == -1) { // no hay grupo para esta linea
                                newDataGroups[newContractId] = {lines:[lineValues]};
                                newContract.push(newContractId);
                            }else{ // ya existe grupo para esta linea
                                newDataGroups[newContractId].lines.push(lineValues);
                            }
                        }
                        linenumber++;
                        return true;
                    });
                    log.debug({title:'updContract', details:dataGroups});
                    log.debug({title:'newContracts', details:newDataGroups});
                    var finalData = []
                    for (var lineContra = 0; lineContra < idsContract.length; lineContra++) {
                        finalData.push(dataGroups[idsContract[lineContra]].lines);
                    }
                    for (var lineNewContract = 0; lineNewContract < newContract.length; lineNewContract++) {
                        finalData.push(newDataGroups[newContract[lineNewContract]].lines);
                    }
                    var allContracts = (finalData.length);
                    log.debug({title:'allContracts', details:allContracts});
                    // log.debug({title:'finalData', details:finalData});
                    for (var contrato = 0; contrato < finalData.length; contrato++) {
                        for (var contratoLine = 0; contratoLine < finalData[contrato].length; contratoLine++) {
                            var datosLine = finalData[contrato][contratoLine];
                            datosLine[12] = allContracts;
                            finalData[contrato][contratoLine] = datosLine;
                            // log.debug({title:'datosLine', details:datosLine});
                        }
                    }
                    log.debug({title:'finalData Transform', details:finalData});
                    return finalData;
                }else{
                    updateTrackingRecord(parameter_record, 6, 'There is no file to process.', '', false);
                }
            } catch (error) {
                log.error({title:'getInputdata', details:error});
                updateTrackingRecord(parameter_record, 5, 'An error occurred while trying to process the file, please try again.', '', false);
            }
        }

        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */
        const map = (mapContext) => {
            try {
                var parameter_record = runtime.getCurrentScript().getParameter({name: "custscript_fb_carte_record_to_process"});
                var datos=JSON.parse(mapContext.value);
                var indice =mapContext.key*1;
                var error = false;
                CONTRACTPAS = CONTRACTPAS + 1;
                // log.debug({title:'Indice: ' + indice, details:datos});
                var idContract, vendorNumber, subsidiary, location, date, shipTo, cusContract;
                for (var linea = 0; linea < datos.length; linea++) {
                    if (linea == 0) {
                        idContract = datos[linea][0];
                        vendorNumber = datos[linea][1];
                        subsidiary = datos[linea][3];
                        location = datos[linea][4];
                        date = datos[linea][5];
                        shipTo = datos[linea][10];
                        cusContract = datos[linea][11];
                        TOTALCONTRACTS = datos[linea][12];
                        // log.debug({title:'InfoToCheck', details:{idContract: idContract, vendorNumber: vendorNumber, subsidiary: subsidiary, location: location, date: date}});
                    }else{
                        if (idContract != datos[linea][0] || vendorNumber != datos[linea][1] || 
                            subsidiary != datos[linea][3] || location != datos[linea][4] || 
                            date != datos[linea][5] || shipTo != datos[linea][10] || 
                            cusContract != datos[linea][11]) {
                            error = true;
                        }
                    }
                }
                updatePercent(TOTALCONTRACTS, CONTRACTPAS);
                if (error) {
                    var notes = '';
                    if (idContract && idContract != '') {
                        notes = '\n The lines do not match for the ID contract: ' + idContract;
                    }else{
                        notes = '\n The lines do not match for the Ship to Contract: ' + shipTo + ' and Vendor: ' + vendorNumber;
                    }
                    log.audit({title:'Error no coinciden lineas en file', details:notes});
                    updateTrackingRecord(parameter_record, 7, notes, '', false);
                }else{
                    var validateResult = validateInformation(datos);
                    // log.debug({title:'validateResult', details:validateResult});
                    if (validateResult.succes == true) {
                        var datosTransform = {trackingRecord: parameter_record, originalData: datos, newData: validateResult.newData};
                        mapContext.write({
                            key:indice,
                            value:datosTransform
                        });
                    }else{
                        var notes = '';
                        if (idContract && idContract != '') {
                            notes = '\n ' + validateResult.error +' for the ID contract: ' + idContract;
                        }else{
                            notes = '\n ' + validateResult.error +' for the Ship to Contract: ' + shipTo + ' and Vendor: ' + vendorNumber;
                        }
                        log.audit({title:'Error al validar infor', details:notes});
                        updateTrackingRecord(parameter_record, 7, notes, '', false);
                    }
                }
            } catch (error) {
                log.error({title:'map', details:error});
            }
        }

        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {
            try {
                CONTRACTPAS = CONTRACTPAS + 1;
                var data = JSON.parse(reduceContext.values);
                var trackingRecord = data.trackingRecord
                var newData = data.newData;
                log.audit({title:'reduce newData: ' + reduceContext.key, details:newData});
                var isNew;
                TOTALCONTRACTS = newData[0][12];
                updatePercent(TOTALCONTRACTS, CONTRACTPAS);
                if (newData[0][0] == '') {
                    isNew = true;
                }else{
                    isNew = false;
                }
                log.debug({title:'isNew?', details:isNew});
                var contractResult;
                if (isNew) {
                    contractResult = createContract(newData);
                    log.audit({title:'contractResult', details:contractResult});
                }else{
                    contractResult = updateContract(newData);
                    log.audit({title:'contractResult_upd', details:contractResult});
                }
                if (contractResult.succes == true) {
                    updateTrackingRecord(trackingRecord, 8, '', contractResult.idContract, false);
                }else{
                    updateTrackingRecord(trackingRecord, 8, contractResult.error, '', false);
                }
            } catch (error) {
                log.error({title:'reduce', details:error});
            }
        }


        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {
            try {
                var parameter_record = runtime.getCurrentScript().getParameter({name: "custscript_fb_carte_record_to_process"});
                log.debug({title:'Final del proceso', details:'trackingRecord: ' + parameter_record});
                var notes = search.lookupFields({
                   type: 'customrecord_fb_uploaded_files',
                   id: parameter_record,
                   columns: ['custrecord_fb_tracking_notes', 'custrecord_fb_tracking_status']
                });
                var status = notes.custrecord_fb_tracking_status[0].value;
                notes = notes.custrecord_fb_tracking_notes;
                if (status!=5) {
                    if (notes != '') {
                        status = 4;
                    }else{
                        status = 3;
                    }
                }
                updateTrackingRecord(parameter_record, status, '', '', false);
                updatePercent(1,1);
                log.audit({title:'Final del summarize', details:'trackingRecord: ' + parameter_record});
            } catch (error) {
                log.error({title:'summarize', details:error});
            }
        }

        function updateContract(datos) {
            var dataReturn = {succes: false, error: '', idContract: ''};
            var transUpdId = datos[0][0];
            try {
                var newLinesIds = [];
                for (var newLineId = 0; newLineId < datos.length; newLineId++) {
                    newLinesIds.push(datos[newLineId][6]);
                }
                // log.debug({title:'AllItemsId', details:newLinesIds});
                var contractObj = record.load({
                    type: record.Type.PURCHASE_CONTRACT,
                    id: transUpdId
                });
                // log.debug({title:'Lines to insert', details:datos.length});
                var contractLine = contractObj.getLineCount({
                    sublistId: 'item'
                });
                // Update existing lines
                for (var actualLine = 0; actualLine < contractLine; actualLine++) {
                    var itemValue = contractObj.getSublistValue({
                        sublistId: 'item',
                        fieldId: 'item',
                        line: actualLine
                    });
                    // log.debug({title:'acual Line: ' + actualLine, details:itemValue});
                    var position = newLinesIds.indexOf(itemValue);
                    if (position != -1) {
                        // log.debug({title:'Data set', details:position});
                        // log.debug({title:'dataSearch', details:datos[position]});
                        var quantityItem = datos[position][8];
                        var rateItem = datos[position][9];
                        contractObj.setSublistValue({
                            sublistId: 'item',
                            fieldId: 'quantity',
                            line: actualLine,
                            value: quantityItem
                        });
                        contractObj.setSublistValue({
                            sublistId: 'item',
                            fieldId: 'rate',
                            line: actualLine,
                            value: rateItem
                        });
                        datos.splice(position, 1);
                        newLinesIds.splice(position, 1);
                    }
                }
                log.debug({title:'NewDatos', details:datos});
                // Add new Lines
                for (var newLine = 0; newLine < datos.length; newLine++) {
                    var dataInLine = datos[newLine];
                    // log.debug({title:'datainLine', details:dataInLine});
                    var idItem = datos[newLine][6];
                    var quantityItem = datos[newLine][8];
                    var rateItem = datos[newLine][9];
                    contractObj.insertLine({
                        sublistId: 'item',
                        line: contractLine
                    });
                    contractObj.setSublistValue({
                        sublistId: 'item',
                        fieldId: 'item',
                        line: contractLine,
                        value: idItem
                    });
                    contractObj.setSublistValue({
                        sublistId: 'item',
                        fieldId: 'quantity',
                        line: contractLine,
                        value: quantityItem
                    });
                    contractObj.setSublistValue({
                        sublistId: 'item',
                        fieldId: 'rate',
                        line: contractLine,
                        value: rateItem
                    });
                    contractLine++;
                }
                // var transId = 269;
                var transId = contractObj.save({
                    enableSourcing: true,
                    ignoreMandatoryFields: true
                });
                dataReturn.succes = true;
                dataReturn.idContract = transId;
            } catch (error) {
                log.error({title:'updateContract', details:error});
                dataReturn.succes = false;
                dataReturn.error = '\n Error updating contract ID: ' + transUpdId;
            }
            return dataReturn
        }

        function createContract(datos) {
            var dataReturn = {succes: false, error:'', idContract: ''}
            try {
                var vendor, subsidiary, location, date, shipTo, custContract;
                var contractObj = record.create({
                    type: record.Type.PURCHASE_CONTRACT,
                    isDynamic: true
                });
                for (var lineData = 0; lineData < datos.length; lineData++) {
                    if (lineData == 0) {
                        vendor = datos[lineData][1];
                        subsidiary = datos[lineData][3];
                        location = datos[lineData][4];
                        date = new Date(datos[lineData][5]);
                        custContract = datos[lineData][11];
                        shipTo = datos[lineData][10];
                        contractObj.setValue({
                            fieldId: 'entity',
                            value: vendor
                        });
                        contractObj.setValue({
                            fieldId: 'subsidiary',
                            value: subsidiary
                        });
                        contractObj.setValue({
                            fieldId: 'location',
                            value: location
                        });
                        contractObj.setValue({
                            fieldId: 'trandate',
                            value: date
                        });
                        contractObj.setValue({
                            fieldId: 'custbody_tkio_hl_customer_contract',
                            value: custContract
                        });
                        contractObj.setValue({
                            fieldId: 'custbody_tkio_hl_ship_to_con',
                            value: shipTo
                        });
                    }
                    var idItem = datos[lineData][6];
                    var quantityItem = datos[lineData][8];
                    var rateItem = datos[lineData][9];
                    // log.debug({title:'Valores to set line: ' + lineData, details:{idItem: idItem, quantityItem: quantityItem, rateItem: rateItem}});
                    contractObj.selectNewLine({
                        sublistId: 'item'
                    });
                    contractObj.setCurrentSublistValue({
                        sublistId: 'item',
                        fieldId: 'item',
                        value: idItem
                    });
                    contractObj.setCurrentSublistValue({
                        sublistId: 'item',
                        fieldId: 'quantity',
                        value: quantityItem
                    });
                    contractObj.setCurrentSublistValue({
                        sublistId: 'item',
                        fieldId: 'rate',
                        value: rateItem
                    });
                    contractObj.commitLine({
                        sublistId: 'item'
                    });
                }
                // var transId = 1507;
                var transId = contractObj.save({
                    enableSourcing: true,
                    ignoreMandatoryFields: true
                });
                dataReturn.succes = true;
                dataReturn.idContract = transId;
            } catch (error) {
                log.error({title:'createContract', details:error});
                dataReturn.succes = false;
                dataReturn.error = '\n Error creating contract for the Ship Contract ID: ' + datos[0][10] + ' and Vendor: ' + datos[0][1];
            }
            return dataReturn;
        }

        function updateTrackingRecord(recordId, status, notes, transaction, clearNotes) {
            try {
                // log.debug({title:'Data to update', details:{recordId: recordId, status: status, notes: notes, transaction: transaction, clearNotes: clearNotes}});
                var recordInfo = search.lookupFields({
                    type: 'customrecord_fb_uploaded_files',
                    id: recordId,
                    columns: ['custrecord_fb_tracking_notes', 'custrecord_fb_tracking_transactions']
                });
                var recordNotes = recordInfo.custrecord_fb_tracking_notes;
                var recordTrans = recordInfo.custrecord_fb_tracking_transactions;
                var allTrans=[];
                if (transaction != '') {
                    allTrans.push(transaction);
                }
                if (recordTrans.length > 0) {
                    for (var index = 0; index < recordTrans.length; index++) {
                        allTrans.push(recordTrans[index].value);
                    }
                }
                if (clearNotes) {
                    recordNotes='';
                    allTrans=[];
                }
                var trackingRecord = record.submitFields({
                    type: 'customrecord_fb_uploaded_files',
                    id: recordId,
                    values: {
                       'custrecord_fb_tracking_status' : status,
                       'custrecord_fb_tracking_notes' : recordNotes + notes,
                       'custrecord_fb_tracking_transactions' : allTrans
                    }
                });
            } catch (error) {
                log.error({title:'updateTrackingRecord', details:error});
            }
        }

        function updatePercent(total, actual) {
            try {
                var percent = 0.0;
                var parameter_record = runtime.getCurrentScript().getParameter({name: "custscript_fb_carte_record_to_process"});
                // log.debug({title:'Datos calc', details:{total: total, contractpas: actual}});
                if (total != 0) {
                    percent = (actual * 100) / total;
                    // log.debug({title:'Recalculando', details:percent});
                }
                var updPercent = record.submitFields({
                    type: 'customrecord_fb_uploaded_files',
                    id: parameter_record,
                    values: {
                        'custrecord_fb_trackin_porcent' : percent + '%'
                    }
                });
            } catch (error) {
                log.error({title:'updatePercent', details:error});
            }
        }

        function validateInformation(datos) {
            var dataReturn = {succes: false, error: '', newData: []}
            try {
                var vendorNumber, subsidiary, location, date, shipTo, cusContract;
                var idSubsidiary, idLocation, idShipTo, idCusContract;
                var itemSKUArray=[];
                var filtersItems = [];
                for (var line = 0; line < datos.length; line++) {
                    vendorNumber=datos[line][1];
                    subsidiary=datos[line][3];
                    location=datos[line][4];
                    date=datos[line][5];
                    shipTo = datos[line][10];
                    cusContract = datos[line][11];
                    itemSKUArray.push(datos[line][6]);
                    filtersItems.push(['name','is',datos[line][6]]);
                    if (line<datos.length-1) {
                        filtersItems.push('OR')
                    }
                }
                // log.debug({title:'datosSearch', details:{vendor: vendorNumber, subsidiary: subsidiary, location: location, items_sku_filter: filtersItems}});
                try {
                    var searchVendor = search.lookupFields({
                       type: search.Type.VENDOR,
                       id: vendorNumber,
                       columns: ['isinactive', 'companyname']
                    });
                    // log.debug({title:'searchVendor', details:searchVendor});
                    if (searchVendor.companyname) {
                        if (searchVendor.isinactive == true) {
                            dataReturn.error = 'There is no active vendor'
                            return dataReturn;
                        }
                    }else{
                        dataReturn.error='There is not a vendor'
                        return dataReturn;
                    }
                } catch (errorVendor) {
                    log.error({title:'validateInformation_vendor', details:errorVendor});
                    dataReturn.succes = false;
                    dataReturn.error= 'Failed to search for vendor';
                    return dataReturn;
                }
                try {
                    var subsidiarySearchObj = search.create({
                        type: search.Type.SUBSIDIARY,
                        filters:
                        [
                            ["name","contains",subsidiary],
                            "AND",
                            ["isinactive","is","F"]
                        ],
                        columns:
                        [
                           search.createColumn({
                              name: "internalid",
                              sort: search.Sort.ASC,
                              label: "ID interno"
                           }),
                           search.createColumn({name: "name", label: "Nombre"})
                        ]
                    });
                    var searchResultCount = subsidiarySearchObj.runPaged().count;
                    // log.debug("subsidiarySearchObj result count",searchResultCount);
                    if (searchResultCount>0) {
                        subsidiarySearchObj.run().each(function(result){
                            idSubsidiary=result.getValue({name: 'internalid'});
                            return true;
                        });
                    }else{
                        dataReturn.error = 'There is no subsidiary';
                        return dataReturn
                    }
                } catch (errorSubsidiary) {
                    log.error({title:'validateInormation_subsidiary', details:errorSubsidiary});
                    dataReturn.succes = false;
                    dataReturn.error= 'Failed to search for subsidiary';
                    return dataReturn;
                }
                try {
                    var locationSearchObj = search.create({
                        type: search.Type.LOCATION,
                        filters:
                        [
                           ["name","is",location], 
                           "AND", 
                           ["isinactive","is","F"], 
                           "AND", 
                           ["subsidiary","anyof",idSubsidiary]
                        ],
                        columns:
                        [
                           search.createColumn({
                              name: "internalid",
                              sort: search.Sort.ASC,
                              label: "ID interno"
                           }),
                           search.createColumn({name: "name", label: "Nombre"}),
                           search.createColumn({name: "phone", label: "Teléfono"}),
                           search.createColumn({name: "city", label: "Ciudad"})
                        ]
                    });
                    var searchResultCount = locationSearchObj.runPaged().count;
                    // log.debug("locationSearchObj result count",searchResultCount);
                    if (searchResultCount>0) {
                        locationSearchObj.run().each(function(result){
                            idLocation = result.getValue({name: 'internalid'});
                            return true;
                        });
                    }else{
                        dataReturn.error='Location is not available';
                        return dataReturn;
                    }
                } catch (errorLocation) {
                    log.error({title:'validateInormation_location', details:errorLocation});
                    dataReturn.succes = false;
                    dataReturn.error= 'Failed to search for location';
                    return dataReturn;
                }
                try {
                    var customerSearchObj = search.create({
                        type: search.Type.CUSTOMER,
                        filters:
                        [
                           ["entityid","is",cusContract]
                        ],
                        columns:
                        [
                           search.createColumn({
                              name: "internalid",
                              sort: search.Sort.ASC,
                              label: "ID interno"
                           }),
                           search.createColumn({name: "altname", label: "Nombre"}),
                           search.createColumn({name: "entityid", label: "ID"}),
                           search.createColumn({name: "addressinternalid", label: "ID interno de dirección"}),
                           search.createColumn({name: "addresslabel", label: "Etiqueta de dirección"})
                        ]
                    });
                    var myPagedData = customerSearchObj.runPaged({
                        pageSize: 1000
                    });
                    if (myPagedData.count > 0) {
                        var labelResult;
                        myPagedData.pageRanges.forEach(function(pageRange){
                            var myPage = myPagedData.fetch({index: pageRange.index});
                            myPage.data.forEach(function(result){
                                labelResult = result.getValue({name: 'addresslabel'});
                                if (labelResult.toLowerCase() == shipTo.toLowerCase()) {
                                    idShipTo = result.getValue({name: 'addressinternalid'});
                                    idCusContract = result.getValue({name: 'internalid'});
                                }
                            });
                        });
                        if (!idShipTo || !idCusContract) {
                            dataReturn.succes = false;
                            dataReturn.error = '"Ship To Contract" and "Customer Contract" do not match on Netsuite';
                            return dataReturn;
                        }
                    }else{
                        dataReturn.error='The customer data is not correct'
                        return dataReturn;
                    }
                } catch (errorShipTo) {
                    log.error({title:'validateInformation_shipTo', details:errorShipTo});
                    dataReturn.succes = false;
                    dataReturn.error = 'Error validating the Ship To';
                    return dataReturn;
                }
                var configRecObj = config.load({
                    type: config.Type.USER_PREFERENCES
                });
                var dateFormat = configRecObj.getValue({
                    fieldId: 'DATEFORMAT'
                });
                var finalDate = moment(date, dateFormat).toDate();
                var isNew = true;
                if (datos[0][0] != '') {
                    isNew = false;
                }
                if (isNew == false) {
                    try {
                        var contractData = search.lookupFields({
                           type: search.Type.PURCHASE_CONTRACT,
                           id: datos[0][0],
                           columns: ['custbody_tkio_hl_customer_contract', 'custbody_tkio_hl_ship_to_con', 'entity', 'trandate', 'subsidiary']
                        });
                        // log.debug({title:'contractData', details:contractData});
                        if (contractData.custbody_tkio_hl_customer_contract && contractData.custbody_tkio_hl_ship_to_con && contractData.entity && contractData.trandate && contractData.subsidiary) {
                            var vendorCon = contractData.entity[0].value;
                            var customerCon = contractData.custbody_tkio_hl_customer_contract[0].value;
                            var shiptoCon = contractData.custbody_tkio_hl_ship_to_con[0].value;
                            var fechaCon = contractData.trandate;
                            var subsidiaryCon = contractData.subsidiary[0].value;
                            // log.debug({title:'Data in Contract', details:{vendorCon: vendorCon, customerCon: customerCon, shiptoCon: shiptoCon, fechaCon: fechaCon, subsidiaryCon: subsidiaryCon}});
                            // log.debug({title:'Data csv', details:{vendorCon: vendorNumber, customerCon: idCusContract, shiptoCon: idShipTo, fechaCon: finalDate, subsidiaryCon: idSubsidiary}});
                            if (vendorCon != vendorNumber) {
                                dataReturn.error='The Vendor does not match on the netsuite contract'
                                return dataReturn;
                            }
                            if (customerCon != idCusContract) {
                                dataReturn.error='The Customer does not match on the netsuite contract'
                                return dataReturn;
                            }
                            if (shiptoCon != idShipTo) {
                                dataReturn.error='The "Ship To" does not match on the netsuite contract'
                                return dataReturn;
                            }
                            if (subsidiaryCon != idSubsidiary) {
                                dataReturn.error='The Subsidiary does not match on the netsuite contract'
                                return dataReturn;
                            }
                        }else{
                            dataReturn.error='The entered contract is not correct'
                            return dataReturn;
                        }
                    } catch (errorContractData) {
                        log.error({title:'errorContractData', details:errorContractData});
                        dataReturn.succes = false;
                        dataReturn.error = 'Error looking up the contract';
                    }
                }
                try {
                    var itemSearchObj = search.create({
                        type: search.Type.ITEM,
                        filters:
                        [
                           filtersItems
                        ],
                        columns:
                        [
                           search.createColumn({
                              name: "internalid",
                              sort: search.Sort.ASC,
                              label: "ID interno"
                           }),
                           search.createColumn({name: "itemid", label: "Nombre"}),
                           search.createColumn({name: "displayname", label: "Nombre para mostrar"}),
                           search.createColumn({name: "salesdescription", label: "Descripción"}),
                           search.createColumn({name: "type", label: "Tipo"}),
                           search.createColumn({name: "baseprice", label: "Precio base"}),
                           search.createColumn({name: "custitem_tkii_comercial_name_hlx", label: "COMERCIAL NAME"})
                        ]
                    });
                    var myPagedData = itemSearchObj.runPaged({
                        pageSize: 1000
                    });
                    // log.debug("itemSearchObj result count",myPagedData.count);
                    if (myPagedData.count == itemSKUArray.length) { // se encontraron todos los articulos
                        myPagedData.pageRanges.forEach(function(pageRange){
                            var myPage = myPagedData.fetch({index: pageRange.index});
                            myPage.data.forEach(function(result){
                                var skuFound = result.getValue({name: 'itemid'});
                                var posicionItem = itemSKUArray.indexOf(skuFound);
                                var itemid = result.getValue({name: 'internalid'});
                                datos[posicionItem][6] = itemid;
                                datos[posicionItem][3] = idSubsidiary;
                                datos[posicionItem][4] = idLocation;
                                datos[posicionItem][5] = finalDate;
                                datos[posicionItem][10] = idShipTo;
                                datos[posicionItem][11] = idCusContract;
                            });
                        });
                        // log.debug({title:'FinalDataMap', details:datos});
                        dataReturn.succes= true;
                        dataReturn.newData= datos;
                    }else{
                        dataReturn.error='Items are not available'
                        return dataReturn;
                    }
                } catch (errorItems) {
                    log.error({title:'validateInformation_items', details:errorItems});
                    dataReturn.succes = false;
                    dataReturn.error = 'Failed to search items';
                }
            } catch (error) {
                log.error({title:'validateInformation', details:error});
                dataReturn.succes = false;
                dataReturn.error= error;
            }
            return dataReturn
        }

        return {getInputData, map, reduce, summarize}

    });
