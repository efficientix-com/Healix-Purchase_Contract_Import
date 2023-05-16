/**
 * @NApiVersion 2.1
 * @NScriptType Suitelet
 * @name FB - Upload Purchase Contract SL.js
 * @version 1.0
 * @author Dylan Mendoza <dylan.mendoza@freebug.mx>
 * @summary This script will manage the custom interface for loading the Purchase contract CSV.
 * @copyright Tekiio México 2023
 * 
 * Client              -> Healix
 * Last modification   -> 16/05/2023
 * Modified by         -> Dylan Mendoza <dylan.mendoza@freebug.mx>
 * Script in NS        -> FB - Upload Purchase Contract SL <ID del registro>
 */
define(['N/file', 'N/https', 'N/log', 'N/record', 'N/redirect', 'N/search', 'N/task', 'N/url', 'N/ui/serverWidget', 'N/runtime'],
    /**
 * @param{file} file
 * @param{https} https
 * @param{log} log
 * @param{record} record
 * @param{redirect} redirect
 * @param{search} search
 * @param{task} task
 * @param{url} url
 * @param{serverWidget} ui
 */
    (file, https, log, record, redirect, search, task, url, ui, runtime) => {
        /**
         * Defines the Suitelet script trigger point.
         * @param {Object} scriptContext
         * @param {ServerRequest} scriptContext.request - Incoming request
         * @param {ServerResponse} scriptContext.response - Suitelet response
         * @since 2015.2
         */
        const onRequest = (scriptContext) => {
            var response = scriptContext.response;
            var request = scriptContext.request;
            try {
                if (request.files['custom_file_upd']) {
                    var archivo = request.files['custom_file_upd'];
                    log.debug({title:'Se envio a procesar', details:archivo});
                    processFile(archivo, response)

                }else{
                    log.debug({title:'Loading interfaz', details:request});
                    var formProcess = createPrincipalForm();
                    response.writePage(formProcess);
                }
            } catch (error) {
                log.error({title:'OnRequest', details:error});
                var formError = createFormError('An error has occurred, contact your administrator.');
                response.writePage(formError);
            }
        }

        function processFile(archivo, response) {
            try {
                if (archivo.fileType != file.Type.CSV) {
                    var formError = createFormError('You inserted a file that is not in CSV format, please replace it.');
                    response.writePage(formError);
                }else{
                    var parameter_folder = runtime.getCurrentScript().getParameter({name: "custscript_fb_csv_folder"});
                    log.debug({title:'Folder', details:parameter_folder});
                    var documento = archivo;
                    var nameDoc = new Date().getTime() + '_' + archivo.name;
                    documento.name = nameDoc;
                    documento.folder = parameter_folder;
                    var doc_id = documento.save();
                    var objRecord = record.create({
                        type: 'customrecord_fb_uploaded_files',
                        isDynamic: true
                    });
                    objRecord.setValue({
                        fieldId: 'custrecord_fb_tracking_csv_file',
                        value: doc_id
                    });
                    objRecord.setValue({
                        fieldId: 'custrecord_fb_tracking_status',
                        value: 1
                    });
                    var trackingId = objRecord.save({
                        enableSourcing: true,
                        ignoreMandatoryFields: true
                    });
                    log.debug({title:'Registro creado', details:trackingId});
                    redirect.toRecord({
                        type: 'customrecord_fb_uploaded_files',
                        id: trackingId
                    });
                }
            } catch (error) {
                log.error({title:'processFile', details:error});
                var formError = createFormError('An error has occurred, contact your administrator.');
                response.writePage(formError);
            }
        }

        function createFormError(msg) {
            try {
                var form = ui.createForm({
                    title: 'Upload Purchase Contract SL'
                });
                var htmlField = form.addField({
                    id: 'custpage_msg',
                    label: ' ',
                    type: ui.FieldType.INLINEHTML
                });
                htmlField.defaultValue = "<div style='background-color: #ffe5e5; border-radius: 10px; border: 3px solid #ffb2b2; padding: 10px 35px; width:100%; height: auto;'>";
                if (!util.isArray(msg)) {
                    var aux = msg;
                    msg = [aux];
                }
                for (var i = 0; i < msg.length; i++) {
                    htmlField.defaultValue += "<p'>" +
                        "<strong style='font-size:15px;'>" +
                        msg[i] +
                        "</strong>" +
                        "</p>";
                }
                htmlField.defaultValue += "</div>";
                return form;
            } catch (error) {
                log.error({ title: 'createFormError', details: e });
                throw "An error has occurred, please try again later.";
            }
        }

        function createPrincipalForm() {
            var form;
            try {
                form = ui.createForm({
                    title: 'Upload Purchase Contract SL'
                });
                // form.clientScriptModulePath = './tkio_reporte_avance_CS.js';
                var empField = form.addField({
                    id: 'custom_file_upd',
                    type: ui.FieldType.FILE,
                    label: 'CSV File'
                });
                empField.updateDisplayType({
                    displayType: ui.FieldDisplayType.NORMAL
                });
                form.addSubmitButton({label: 'Upload'});
            } catch (error) {
                log.error({title:'createPrincipalForm', details:error});
                form = createFormError('An error occurred while trying to create the screen');
            }
            return form;
        }

        return {onRequest}

    });
