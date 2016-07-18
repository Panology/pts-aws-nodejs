/* index.js
 * Common PTS Library - Entry Point
 */

/** Private **/

/** Public **/
module.exports = {

    // AWS Auth module
    Auth: require('./lib/aws-auth.js'),

    // S3 Get Log module
    GetLog: require('./lib/s3-getlog.js'),

    // S3 Put module
    PutS3: require('./lib/s3-put.js'),

    // ElasticSearch Send module
    SendToES: require('./lib/es-send.js'),

    // ElasticSearch Index module
    ESIndex: require('./lib/es-index.js')

};
