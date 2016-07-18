/* es-index.js
 * A library for handling ElasticSearch indexes
 * TODO: Standardize method arguments
 */

// Required Modules
var AWS = require('aws-sdk');
var path = require('path');
var ES = require('elasticsearch');
var PTSAuth = require('./aws-auth.js');

/********************************************
 * Constructor
 * @param options - (optional) Configuration
 *******************************************/
module.exports = ESIndex;
function ESIndex(options){
    // Debug
    if (options && options.debug) console.log('ESIndex():constructor():: Options = '
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
     * checkIndex
     * Check to see if the given index exists in the ElasticSearch (ES) domain
     *
     * @param idx    The document to send to ES
     * @param cntxt  The callers' context
     * @param credz  (optional) The credentials to use
     * @param cb     (optional) The callback to pass execution to
     * @param rest   (optional) Arguments to pass along to the callback
     */
    this.checkIndex = function checkIndex(idx, cntxt, credz, cb, ...rest){

        // Setup debug in the AWS library
        if (debug>1){
            AWS.config.update({logger: process.stdout});
        }

        // Handle arguments
        if (cntxt) context = cntxt;
        if (credz) creds = credz;
        if (cb)    callback = cb;

        // We must have a valid config for this to work
        if (!config || !config.service || !config.endpoint){
            throw 'checkIndex() requires a valid config';
        }

        // Sanity check our index argument
        if (!idx){
            throw 'checkIndex() requires a valid index';
        }

        if (debug) {
            console.log('checkIndex():: Using config: ' + JSON.stringify(config, null, 2));
            console.log(`checkIndex():: Checking for index: ${idx}`);
        }

        // Make sure we have a valid creds
        if (!creds){
            console.log('checkIndex():: Obtaining credentials ourself');
            var auth = new PTSAuth({debug: debug});
            auth.getCreds((credz)=>{
                creds = credz;
            });
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
        if (debug) console.log('checkIndex():: ES Client:Credentials: ' + JSON.stringify(es.transport._config.amazonES.credentials, null, 2));

        // Check to see if the index exists
        es.indices.exists({
            index: idx
        }).then(
            function(exists){
                if (exists) {
                    if (debug) console.log('checkIndex():indices.exists():: Index exists.');
                } else {
                    if (debug) console.log('checkIndex():indices.exists():: Index does not exist.');
                }
                callback(exists);
                return;
            }, function(error){
                if (error) console.log('checkIndex():indices.exists():: ES error: ' + error);
            }
        ); // END: Check to see if the index exists
    } // END: checkIndex

    /**
     * createIndex
     * Create the given index in the ElasticSearch (ES) domain
     *
     * @param idx    The document to send to ES
     * @param cntxt  The callers' context
     * @param credz  (optional) The credentials to use
     * @param cb     (optional) The callback to pass execution to
     * @param rest   (optional) Arguments to pass along to the callback
     */
    this.createIndex = function createIndex(idx, cntxt, credz, cb, ...rest){

        // Setup debug in the AWS library
        if (debug>1){
            AWS.config.update({logger: process.stdout});
        }

        // Handle arguments
        if (cntxt) context = cntxt;
        if (credz) creds = credz;
        if (cb)    callback = cb;

        // We must have a valid config for this to work
        if (!config || !config.service || !config.endpoint){
            throw 'createIndex() requires a valid config';
        }

        // Sanity check our index argument
        if (!idx){
            throw 'createIndex() requires a valid index';
        }

        if (debug) {
            console.log('createIndex():: Using config: ' + JSON.stringify(config, null, 2));
            console.log(`createIndex():: Creating index: ${idx}`);
        }

        // Make sure we have a valid creds
        if (!creds){
            console.log('createIndex():: Obtaining credentials ourself');
            var auth = new PTSAuth({debug: debug});
            auth.getCreds((credz)=>{
                creds = credz;
            });
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
        if (debug) console.log('createIndex():: ES Client:Credentials: ' + JSON.stringify(es.transport._config.amazonES.credentials, null, 2));

        // Create the index
        es.indices.create({
            index: idx
        }).then(
            function(response){
                if (response) {
                    if (debug) console.log('createIndex():indices.create():: ES Create Index response: ' + JSON.stringify(response, null, 2));
                } else {
                    console.log('createIndex():indices.create():: ES Create Index gave an empty response.');
                }
                callback();
                return;
            }, function(error){
                if (error) console.log('createIndex():indices.create():: ES error: ' + error);
            }
        ); // END: Create the index
    } // END: createIndex

}; // END: module.exports

// END: es-index.js
