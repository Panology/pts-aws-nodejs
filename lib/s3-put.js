/* s3-put.js
 * A library for adding objects to S3
 * TODO: Standardize method arguments
 */

// Required Modules
var AWS = require('aws-sdk');

/********************************************
 * Constructor
 * @param options - (optional) Configuration
 *******************************************/
module.exports = PutS3;
function PutS3(options){
    // Debug
    if (options && options.debug) console.log('PutS3():constructor():: Options = '
        + JSON.stringify(options, null, 2));

    /********************************************
     * Private Methods/Properties
     * Only accessible to Privileged methods
     *******************************************/
    var debug = (options && options.debug !== undefined) ? options.debug : 0;
    var creds = (options && options.creds !== undefined ) ? options.creds : null;
    var context = (options && options.context !== undefined) ? options.context : null;
    var callback = function(){ throw 'GetLog():: callback missing'; }

    /********************************************
     * Privileged
     * Has access to Private properties/methods
     * Accessible from public
     *******************************************/

    /**
     * putObject
     * Add an object to S3
     *
     * @param object  The object to store in S3
     * @param cntxt   The callers' context
     * @param credz   The credentials to use
     * @param cb      The callback function to invoke
     * @param rest    (optional) Arguments to pass along to the callback
     */
    this.putObject = function putObject(obj, cntxt, credz, cb, ...rest){

        // Setup debug in the AWS library
        if (debug>1){
            AWS.config.update({logger: process.stdout});
        }

        // Verify arguments
        if (!obj)
            throw 'putObject() requires an object';
        if (typeof(cb) !== "function")
            throw 'putObject() requires callback';

        // Store our parameters into private properties
        callback = cb;
        creds = credz;
        if (typeof(cntxt) == "function"){
            context = cntxt;
        }

        // Debug: Show our arguments
        if (debug){
            var numRestArgs = rest !== undefined ? rest.length : 0;
            console.log('putObject():: Called with ' + (numRestArgs+3) + ' arguments');
            console.log('putObject():: Object = ' + JSON.stringify(obj, null, 2));
            console.log('putObject():: Context = Function | ' + context.name);
            console.log('putObject():: Credz = Object | ' + JSON.stringify(creds, null, 2));
            console.log('putObject():: Callback = Function | ' + callback.name);
            for (var i=0, len=numRestArgs; i<len; i++){
                if (typeof rest[i] == "function"){
                    console.log('getLogFromS3():: Arg ' + (i+2) + ': function | ' + rest[i].name);
                } else {
                    console.log('getLogFromS3():: Arg ' + (i+2) + ': ' + typeof rest[i]
                        + ' | ' + JSON.stringify(rest[i], null, 2));
                }
            }
        }

        // For now, we assume creds have already been resolved
        // TODO: Expand putObject to include optional creds resolution

        // If creds ends up being NULL, AWS.S3 will chose it's own defaults
        var S3 = new AWS.S3({credentials: creds});

        // Upload the object to S3
        S3.upload(obj, (err, data) => {
            if (err){
                console.log(`putObject():S3.upload():: Error - ${err}`);
                context.fail('putObject():: Error uploading object');
            }
            if (data && debug){
                console.log('putObject():S3.upload():: Response - ' +
                    JSON.stringify(data, null, 2));
            }
            callback(obj, context, creds, rest);
        });

    } // END: putObject

}; // END: module.exports

// END: s3-put.js
