/* aws-auth.js
 * A helper library for wrapping auth around AWS
 * TODO: Standardize method arguments
 */

// Required Modules
var AWS = require('aws-sdk');
var debug = 1; // 0 = on | 1 = off | 2 = trace

/********************************************
 * Constructor
 * @param options - (optional) Configuration
 *******************************************/
module.exports = Auth;
function Auth(options){
    // Debug
    if (options && options.debug) console.log('Auth():constructor():: Options = '
        + JSON.stringify(options, null, 2));

    /********************************************
     * Private Methods/Properties
     * Only accessible to Privileged methods
     *******************************************/
    var debug = (options && options.debug !== undefined) ? options.debug : 0;
    var credChain = (options && options.credChain !== undefined ) ? options.credChain : null;

    /********************************************
     * Privileged
     * Has access to Private properties/methods
     * Accessible from public
     *******************************************/

    /**
     * Create a credential chain, resovle it, then call the next function.
     *
     * @param callback   Funciton to call once credentials have been obtained
     * @param credChain  Chain to use in obtaining a credential, or NULL to use default
     * @param rest       (optional) A variable number of arguments to pass along to the callback
     */
    this.getCreds = function getCreds(callback, credChain, ...rest){

        // Setup debug in the AWS library
        if (debug>1){
            AWS.config.update({logger: process.stdout});
        }

        // Verify arguments
        if (typeof(callback) !== 'function'){
            console.log('getCreds():: Missing callback - ' + callback);
            throw 'getCreds() requires callback';
        }
        if (credChain && !(credChain instanceof AWS.CredentialProviderChain)) {
            throw 'getCreds() called with an invalid credChain';
        }

        // Debug: Show our arguments
        if (debug){
            var numRestArgs = rest !== undefined ? rest.length : 0;
            console.log('getCreds():: Called with ' + (numRestArgs+2) + ' arguments');
            console.log('getCreds():: Callback: ' + callback.name);
            console.log('getCreds():: credChain: ' + JSON.stringify(credChain, null, 2));
            for (var i=0, len=numRestArgs; i<len; i++){
                if (typeof rest[i] == "function"){
                    console.log('getCreds():: Arg ' + (i+3) + ': function | ' + rest[i].name);
                } else {
                    console.log('getCreds():: Arg ' + (i+3) + ': ' + typeof rest[i] + ' | ' + JSON.stringify(rest[i], null, 2));
                }
            }
        }

        // If we were not given as chain, use the default
        if (!credChain) credChain = new AWS.CredentialProviderChain();

        // Now resolve the chain & call next
        console.log('getCreds():: Resolving credential chain');
        credChain.resolve( function (err, cred){
            if (err) {
                throw 'getCreds():: Encountered an error attempting to resovle credentials from the default chain. Error: ' + err;
            } else {
                if (cred) {
                    if (debug) console.log('getCreds():: Calling next: ' + callback.name);
                    callback(cred, rest);
                } else {
                    throw 'getCreds():: Unable to resovle credentials from the default chain, aborting.';
                }
            }
        }); // END: credChain.resolve()

    }; // END: getCreds()

}
// END: aws-auth.js
