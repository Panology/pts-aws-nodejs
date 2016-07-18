/* es-send.js
 * A library for sending documents to ElasticSearch
 * TODO: Standardize method arguments
 */

// Required Modules
var AWS = require('aws-sdk');
AWS.config.update({logger: process.stdout});
var path = require('path');
var ES = require('elasticsearch');

/********************************************
 * Constructor
 * @param options - (optional) Configuration
 *******************************************/
module.exports = SendToES;
function SendToES(options){
    // Debug
    if (options && options.debug) console.log('SendToES():constructor():: Options = '
        + JSON.stringify(options, null, 2));

    /********************************************
     * Private Methods/Properties
     * Only accessible to Privileged methods
     *******************************************/
    var debug = (options && options.debug !== undefined) ? options.debug : 0;
    var creds = (options && options.creds !== undefined ) ? options.creds : null;
    var context = (options && options.context !== undefined) ? options.context : null;
    var config = (options && options.config !== undefined) ? options.config : null;
    var callback = function(){ if (debug) console.log('SendToES():: callback missing'); }

    /********************************************
     * Privileged
     * Has access to Private properties/methods
     * Accessible from public
     *******************************************/

    /**
     * sendDoc
     * Send the given document to ElasticSearch (ES)
     * If all events are successfully indexed, indicate success back to Lambda
     * (using the "context" parameter).
     *
     * @param doc    The document to send to ES
     * @param cntxt  The callers' context
     * @param credz  (optional) The credentials to use
     * @param cb     (optional) The callback to pass execution to
     * @param rest   (optional) Arguments to pass along to the callback
     */
    this.sendDoc = function sendDoc(doc, cntxt, credz, cb, ...rest){
        if (cntxt) context = cntxt;
        if (credz) creds = credz;
        if (cb)    callback = cb;

        // We must have a valid config for this to work
        if (!config || !config.service || !config.searchby || !config.index){
            throw 'sendDoc() requires a valid config';
        }
        if (debug) {
            console.log('sendDoc():: Using config:' + JSON.stringify(config, null, 2));
            console.log('sendDoc():: Document: ' + JSON.stringify(doc, null, 2));
        }

        // Sanity check our document
        var idx = doc[config.searchby];
        if (idx==null){ // This *could* be 0 (zero), so we must be more explicit
            console.log('sendDoc():: Document does not contain required attribute - ' + config.searchby);
            console.log(`sendDoc():: Document Index: ${idx}`);
            return;
        }

        // Create our ES Client
        var esLog = 'warning';
        if (debug) esLog = 'debug';
        var es = ES.Client({
            hosts: config.endpoint,
            log: esLog,
            connectionClass: require('http-aws-es'),
            amazonES: {
                region: config.region,
                credentials: creds
            }
        });
        if (debug) console.log('sendDoc():: ES Client:Credentials: ' + JSON.stringify(es.transport._config.amazonES.credentials, null, 2));

        // We must have a valid ID in our document
        // if (!doc.eventID){
        //     throw 'sendDoc() requires a valid eventID in the document';
        // }
        if (debug) console.log('sendDoc():: Document ID: ' + doc[config.searchby]);

        // First we need to see if our document already exists in the index
        // - This returns a promise, which we chain into via `.then()`
        es.search({
            index: config.index,
            q: '_id:' + doc[config.searchby]
        }).then(

            // The search returned cleanly
            function(response){
                if (response) {
                    if (debug) console.log('sendDoc():search():: ES Search response: ' + JSON.stringify(response, null, 2));

                    // Document already exists
                    if (response.hits.total > 0){
                        // So we will update it
                        console.log(`sendDoc():update():: Updating doc in index - ${config.index}`);
                        console.log(`sendDoc():update():: Updating doc with id - ${doc[config.searchby]}`);
                        if (debug)
                            console.log(`sendDoc():update():: Updating doc - ${doc}`);
                        es.update({
                            index: config.index,
                            type: config.doctype,
                            id: doc[config.searchby],
                            body: { doc: doc }
                        }).then( // Our (inline) ES Update Callback
                            function(response){
                                if (response) console.log('sendDoc():update():: ES Update response: ' + JSON.stringify(response, null, 2));
                                callback(context, rest);
                            },
                            function(error){
                                if (error){
                                    if (typeof(error) == 'object'){
                                        console.log('sendDoc():update():: ES Update error: '
                                            + JSON.stringify(error, null, 2));
                                    } else {
                                        console.log('sendDoc():update():: ES Update error: ' + error);
                                    }
                                    context.fail('sendDoc()::update():: ES Update failed.');
                                }
                            }
                        );

                    // Document does not exist (yet)
                    } else {

                        // So we will create it
                        console.log(`sendDoc():create():: Creating doc in index - ${config.index}`);
                        console.log(`sendDoc():create():: Creating dock with id - ${doc[config.searchby]}`);
                        if (debug)
                            console.log(`sendDoc():create():: Creating doc - ${doc}`);
                        es.create({
                            index: config.index,
                            type: config.doctype,
                            id: doc[config.searchby],
                            body: doc
                        }).then( // Our (inline) ES Create Callback
                            function(response){
                                if (response) console.log('sendDoc():create():: ES Create response: ' + JSON.stringify(response, null, 2));
                                callback(context, rest);
                            }, function(error){
                                if (error){
                                    if (typeof(error) == 'object'){
                                        console.log('sendDoc():create():: ES Create error: ' + JSON.stringify(error, null, 2));
                                    } else {
                                        console.log('sendDoc():create():: ES Create error: ' + error);
                                    }
                                    context.fail('sendDoc():create():: ES Create failed.');
                                }
                            }
                        );
                    }

                // This shouldn't happen
                } else {
                    console.log('sendDoc():search():: ES Search no response - this should not happen');
                }

            // There was an error searching for the document
            }, function(error){
                if (error) console.log('sendDoc():search():: ES Search error: ' + error);
            }
        ); // END: See if our document already exists in the index

    } // END: sendDoc

}; // END: module.exports

// END: es-send.js
