/* s3-getlog.js
 * A library for processing log files from S3
 * TODO: Standardize method arguments
 */

// Required Modules
var AWS = require('aws-sdk');
var PTSAuth = require('./aws-auth.js');
var zlib = require('zlib');
var byline = require('byline');
var stream = require('stream');

/********************************************
 * Constructor
 * @param options - (optional) Configuration
 *******************************************/
module.exports = GetLog;
function GetLog(options){
    // Debug
    if (options && options.debug) console.log('GetLog():constructor():: Options = '
        + JSON.stringify(options, null, 2));

    /********************************************
     * Private Methods/Properties
     * Only accessible to Privileged methods
     *******************************************/
    var debug = (options && options.debug !== undefined) ? options.debug : 0;
    var creds = (options && options.creds !== undefined ) ? options.creds : null;
    var context = (options && options.context !== undefined) ? options.context : null;
    var log = {};
    var numRecords = 0;
    var isDone = false;
    var callback = function(){ throw 'GetLog():: callback missing'; }

    /**
     * readLogArray
     * Read the contents of a log file from S3, passing the record(s) off to
     * a callback function. This function assumes the log contains a single
     * JSON Array with 1 or more Records.
     *
     * @param credz  The credentials to use
     * @param rest   (optional) Arguments to pass along to the callback
     */
    readLogArray = function readLogArray(credz, ...rest){

        // Handle our Arguments
        if (credz) creds = credz;
        var cbArgs = rest;

        // If creds ends up being NULL, AWS.S3 will chose it's own defaults
        var S3 = new AWS.S3({credentials: creds});

        // Read the log from S3 as a stream
    	var s3Stream = S3.getObject({Bucket: log.bucket, Key: log.key})
            .createReadStream()
            .pipe(zlib.createGunzip());
        console.log('readLogArray():: Created s3Stream');

        console.log('readLogArray():: Setting up listeners on s3Stream');

        // Read the S3 stream into a string
        var jsonString = '';
        s3Stream.on('data', (data) => {
            console.log('readLogArray():s3Stream.data():: Received chunk');
            jsonString += data.toString();
        });
        if (debug) console.log('readLogArray():: Listener added to: data');

        // On Error reading the stream from S3
        s3Stream.on('error', () => {
            console.log('readLogArray():s3Stream.error():: Error getting object "' + log.key
                + '" from bucket "' + log.bucket + '".  Make sure they exist and your bucket is'
                + ' in the same region as this function.' );
            context.fail('readLogArray():: Error processing s3Stream');
        });
        if (debug) console.log('readLogArray():: Listener added to: error');

        // When done reading the stream from S3
        s3Stream.on('end', () => {
            console.log('readLogArray():s3Stream.end():: Done reading stream');
            if (debug) console.log('readLogArray():s3Stream.end():: JSON Source: ' + jsonString);

            // Convert string into an object
            var oLog = JSON.parse(jsonString);

            // Iterate over all records in this log
            numRecords = oLog.Records.length;
        	for (var i=0, len = oLog.Records.length; i<len; i++){
                console.log('readLogArray():s3Stream.end():: Sending log record ' + (i+1)
                    + ' to function: ' + callback.name);

                // Pass this record on to the callback function
                callback(oLog.Records[i], context, creds, cbArgs);
            }

            // Done sending events from this log
            console.log('readLogArray():s3Stream.end():: Finished processing ' + numRecords
                + ' records.');
            isDone = true;
            return;
        });
        if (debug) console.log('readLogArray():: Listener added to: end');

        console.log('readLogArray():: Returning');
        return;
    } // END: readLogArray

    /**
     * readLogLines
     * Read the contents of a log file from S3, passing the record(s) off to
     * a callback function. This function assumes the log contains a single
     * JSON object per line.
     *
     * @param credz  The credentials to use
     * @param rest   (optional) Arguments to pass along to the callback
     */
    readLogLines = function readLogLines(credz, ...rest){

        // Handle our Arguments
        if (credz) creds = credz;
        var cbArgs = rest;

        if (debug)
            console.log('readLogLines():: Callback: ' +  callback.name);

        // If creds ends up being NULL, AWS.S3 will chose it's own defaults
        var S3 = new AWS.S3({credentials: creds});

        // Setup to read lines into records
        var lineStream = new byline.LineStream();
        var recordStream = new stream.Transform({objectMode: true});
        recordStream._transform = function(line, encoding, done) {
            var logRecord = JSON.parse(line.toString());
            this.push(logRecord);
            done();
        }

        // Read the log from S3 as a stream
        var s3Stream = S3.getObject({Bucket: log.bucket, Key: log.key})
            .createReadStream()
            .pipe(zlib.createGunzip())
            .pipe(lineStream)
            .pipe(recordStream);
        console.log('readLogLines():: Created s3Stream');

        // On Error reading the stream from S3
        s3Stream.on('error', function() {
            console.log('readLogLines():s3Stream.error():: Error getting object "' + log.key
                + '" from bucket "' + log.bucket + '".  Make sure they exist and your bucket is'
                + ' in the same region as this function.' );
            context.fail('readLogLines():: Error processing s3Stream');
        });
        if (debug) console.log('readLogLines():: Listener added to: error');

        // Handle each record as it is emitted
        s3Stream.on('data', (parsedEntry) => {
            numRecords++;
            if (debug) {
                console.log('readLogLines():: Parsing line ' + numRecords);
                console.log('readLogLines():: Sending to: ' + callback.name);
            }

            // Pass this record on to the callback function
            callback(parsedEntry, context, creds, cbArgs);
        });
        if (debug) console.log('readLogLines():: Listener added to: data');

        // When done reading the stream from S3
        s3Stream.on('end', () => {
            isDone = true;
            console.log(`readLogLines():: Done reading ${numRecords} lines`);
        });
        if (debug) console.log('readLogLines():: Listener added to: end');

        console.log('readLogLines():: Returning');
        return;
    } // END: readLogLines

    /********************************************
     * Privileged
     * Has access to Private properties/methods
     * Accessible from public
     *******************************************/

    /**
     * getLogFromS3
     * Request a log file to be read from S3. This function wraps the call to
     * readLog() with AWS Credentials. The readLog() function does the actual
     * reading of the log file.
     *
     * @param s3Log    The log from S3 to retrieve (s3Log.bucket & s3Log.key)
     * @param cb       The callback function to invoke
     * @param cntxt    (optional) The callers' context
     * @param rest     (optional) The rest of the arguments, passed along to the callback
     */
    this.getLogFromS3 = function getLogFromS3(s3Log, cb, cntxt, ...rest){

        // Setup debug in the AWS library
        if (debug>1){
            AWS.config.update({logger: process.stdout});
        }

        // Verify arguments
        if (!s3Log || !s3Log.bucket || !s3Log.key)
            throw 'getLogFromS3() requires S3 Log';
        if (typeof(cb) !== "function")
            throw 'getLogFromS3() requires callback';

        // Store our parameters into private properties
        log = s3Log; // Stored for later use
        callback = cb;
        if (typeof(cntxt) == "function"){
            context = cntxt;
        }

        // Debug: Show our arguments
        if (debug){
            var numRestArgs = rest !== undefined ? rest.length : 0;
            console.log('getLogFromS3():: Called with ' + (numRestArgs+3) + ' arguments');
            console.log('getLogFromS3():: Log = ' + JSON.stringify(log, null, 2));
            console.log('getLogFromS3():: Callback = Function | ' + callback.name);
            console.log('getLogFromS3():: Context = Function | ' + context.name);
            for (var i=0, len=numRestArgs; i<len; i++){
                if (typeof rest[i] == "function"){
                    console.log('getLogFromS3():: Arg ' + (i+2) + ': function | ' + rest[i].name);
                } else {
                    console.log('getLogFromS3():: Arg ' + (i+2) + ': ' + typeof rest[i]
                        + ' | ' + JSON.stringify(rest[i], null, 2));
                }
            }
        }

        /* We currently support the following S3 Log types:
         *   Array - (default) A single JSON Array containing 1 or more Records
         *   Lines - 1 JSON object per line
         */
        console.log('getLogFromS3():: Determining which readLog to call');
        var readLog = null;
        if (log.type == 'Lines')
            readLog = readLogLines;
        else if (log.type == 'Array')
            readLog = readLogArray;
        else {
            console.log('getLogFromS3():: Unknown or unspecified s3Log.type '
                + `[${log.type}], assuming Array`);
            readLog = readLogArray;
        }
        if (debug)
            console.log('getLogFromS3():: readLog => ' + readLog.name);

        console.log('getLogFromS3():: Determining how to call readLog(), direct or via Auth');

        // If we have a Credentials, we assume it is already populated
        if (creds && creds instanceof AWS.Credentials){
            if (debug) console.log('getLogFromS3():: Calling readLog() directly');
            readLog(creds, rest);
        }

        // If we have a CredentialProviderChain, we assume we need to resolve it first
        if (creds && creds instanceof AWS.CredentialProviderChain){
            if (debug)
                console.log('getLogFromS3():: Calling readLog() via PTSAuth.getCreds() using creds');
            if (debug>1)
                console.log('getLogFromS3():: Creds = ' + JSON.stringify(creds, null, 2));
            var auth = new PTSAuth({debug: debug});
            auth.getCreds(readLog, creds, context, rest);
        }

        // If we have nothing, we resolve credetials using defaults
        if (!creds){
            if (debug)
                console.log('getLogFromS3():: Calling readLog() via PTSAuth.getCreds() using defaults');
            var auth = new PTSAuth({debug: debug});
            auth.getCreds(readLog, null, context, rest);
        }

        console.log('getLogFromS3():: Returning.');
        return;
    } // END: getLogFromS3

    /**
     * getNumRecords
     * Get the current number of records
     */
    this.getNumRecords = function getNumRecords(){ return numRecords; }

    /**
     * isDone
     * Have all records been read?
     */
    this.isDone = function isDone(){ return isDone; }

}; // END: module.exports

// END: s3-getlog.js
