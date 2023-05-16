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
define(['N/file', 'N/https', 'N/log', 'N/record', 'N/redirect', 'N/search', 'N/task', 'N/url'],
    /**
 * @param{file} file
 * @param{https} https
 * @param{log} log
 * @param{record} record
 * @param{redirect} redirect
 * @param{search} search
 * @param{task} task
 * @param{url} url
 */
    (file, https, log, record, redirect, search, task, url) => {
        /**
         * Defines the Suitelet script trigger point.
         * @param {Object} scriptContext
         * @param {ServerRequest} scriptContext.request - Incoming request
         * @param {ServerResponse} scriptContext.response - Suitelet response
         * @since 2015.2
         */
        const onRequest = (scriptContext) => {
            try {
                
            } catch (error) {
                log.error({title:'OnRequest', details:error});
            }
        }

        return {onRequest}

    });
