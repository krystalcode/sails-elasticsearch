/**
 * Module Dependencies
 */

var elasticsearch = require('elasticsearch');

/**
 * Connection.js
 *
 * Handles connecting and disconnecting from an elasticsearch server.
 *
 * @param {Object} config
 * @param {Function} callback
 */

var Connection = module.exports = function(config, cb) {

  var self = this;

  // Ensure something is set for config
  this.config = config || {};

  // Hold the connection
  this.connection = {};

  // Create a new Connection
  this.connect(function(err, client) {
    if(err) return cb(err);
    self.connection = client;
    cb(null, self);
  });

};


///////////////////////////////////////////////////////////////////////////////////////////
/// PUBLIC METHODS
///////////////////////////////////////////////////////////////////////////////////////////


/**
 * Connect to the elasticsearch server
 *
 * @param {Function} callback
 * @api public
 */

Connection.prototype.connect = function(cb) {
  /**
   * @Issue(
   *   "Check if the connection is initialised when the client is created, and
   *   if so, react on 'ready' and 'error' events"
   *   type="bug"
   *   priority="low"
   * )
   */
  var client = new elasticsearch.Client(this.config);
  cb(null, client);
};
