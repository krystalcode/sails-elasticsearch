/**
 * Module Dependencies
 */

var async      = require('async');
var Connection = require('./connection');

/**
 * Helper functions for getting the elasticsearch client, and for determining
 * the index and type that will be used for the operation at hand.
 */

var getClient = function(connections, connectionName) {
  return connections[connectionName].connection.connection;
};

var getIndex = function(connections, connectionName, collectionName, index) {
  /**
   * @Issue(
   *   "Allow dynamic index creation based on the current user, time or
   *   other logic"
   *   type="improvement"
   *   priority="normal"
   * )
   */
  if (index == undefined) {
    var config = connections[connectionName].config;
    if (config.index != undefined) {
      index = config.index;
    } else {
      index = collectionName;
    }
  }

  return index;
}

var getType = function(connections, connectionName, collectionName) {
  return connections[connectionName].collections[collectionName].adapter.identity;
}

// Helper function for getting the attributes of the given collection.
var getAttributes = function(connections, connectionName, collectionName) {
  return connections[connectionName].collections[collectionName]._attributes;
}

// Helper function for parsing an Elastic Search response and getting the
// record values as json objects.
var getHits = function(esResponse, cb) {
  var results = [];

  if (esResponse.hits.total == 0) {
    return results;
  }

  async.each(esResponse.hits.hits, function(e, c) {
    var result = e._source;
    result['id'] = e._id;
    results.push(result);
    c();
  }, function(err) {
    if (err) return cb(err);
    cb(null, results);
  });
}

// Restrict sub-attributes in attributes of type 'json' to the ones
// defined by the 'restrictAttributes' and 'skipAttributes' properties.
var restrictAttributes = function(attributes, values) {
  Object.keys(attributes).forEach(function(attribute) {
    if (attributes[attribute].type != undefined && attributes[attribute].type == 'json') {
      // Remove attributes that are not included in the restrictAttributes
      // property, if defined.
      if (attributes[attribute].restrictAttributes) {
        Object.keys(values[attribute]).forEach(function(e) {
          for (var i in attributes[attribute].restrictAttributes) {
            if (e != attributes[attribute].restrictAttributes[i]) {
              delete values[attribute][e];
            }
          }
        });
      }

      // Remove attributes included in the skipAttributes property, if defined.
      if (attributes[attribute].skipAttributes) {
        Object.keys(values[attribute]).forEach(function(e) {
          for (var i in attributes[attribute].skipAttributes) {
            if (e == attributes[attribute].skipAttributes[i]) {
              delete values[attribute][e];
            }
          }
        });
      }
    }
  });

  return values;
}

/**
 * waterline-elasticsearch
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters, you may need more flexiblity.
 * In any case, it's probably a good idea to start with one file and refactor only if necessary.
 * If you do go that route, it's conventional in Node to create a `./lib` directory for your private submodules
 * and load them at the top of the file with other dependencies.  e.g. var update = `require('./lib/update')`;
 */
module.exports = (function () {


  // You'll want to maintain a reference to each connection
  // that gets registered with this adapter.
  var connections = {};



  // You may also want to store additional, private data
  // per-connection (esp. if your data store uses persistent
  // connections).
  //
  // Keep in mind that models can be configured to use different databases
  // within the same app, at the same time.
  //
  // i.e. if you're writing a MariaDB adapter, you should be aware that one
  // model might be configured as `host="localhost"` and another might be using
  // `host="foo.com"` at the same time.  Same thing goes for user, database,
  // password, or any other config.
  //
  // You don't have to support this feature right off the bat in your
  // adapter, but it ought to get done eventually.
  //

  var adapter = {

    identity: 'sails-elasticsearch',

    // Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
    // If true, the schema for models using this adapter will be automatically synced when the server starts.
    // Not terribly relevant if your data store is not SQL/schemaful.
    //
    // If setting syncable, you should consider the migrate option,
    // which allows you to set how the sync will be performed.
    // It can be overridden globally in an app (config/adapters.js)
    // and on a per-model basis.
    //
    // IMPORTANT:
    // `migrate` is not a production data migration solution!
    // In production, always use `migrate: safe`
    //
    // drop   => Drop schema and data, then recreate it
    // alter  => Drop/add columns as necessary.
    // safe   => Don't change anything (good for production DBs)
    //
    syncable: false,

    schema: false,


    // Let's keep the defaults provided by the 'elasticsearch' client
    defaults: {},



    /**
     *
     * This method runs when a model is initially registered
     * at server-start-time.  This is the only required method.
     *
     * @param  {[type]}   connection [description]
     * @param  {[type]}   collection [description]
     * @param  {Function} cb         [description]
     * @return {[type]}              [description]
     */
    registerConnection: function(connection, collections, cb) {

      if(!connection.identity) return cb(new Error('Connection is missing an identity.'));
      if(connections[connection.identity]) return cb(new Error('Connection is already registered.'));

      // Add in logic here to initialize connection
      // e.g. connections[connection.identity] = new Database(connection, collections);
      connections[connection.identity] = {
        config: connection,
        collections: collections || {}
      };

      new Connection(connection, function(err, conn) {
        if (err) return cb(err);

        connections[connection.identity].connection = conn;
        cb(null, conn);
      });
    },


    /**
     * Fired when a model is unregistered, typically when the server
     * is killed. Useful for tearing-down remaining open connections,
     * etc.
     *
     * @param  {Function} cb [description]
     * @return {[type]}      [description]
     */
    // Teardown a Connection
    teardown: function (conn, cb) {

      if (typeof conn == 'function') {
        cb = conn;
        conn = null;
      }
      if (!conn) {
        connections = {};
        return cb();
      }
      if(!connections[conn]) return cb();
      delete connections[conn];
      cb();
    },


    // Return attributes
    describe: function (connection, collection, cb) {
			// Add in logic here to describe a collection (e.g. DESCRIBE TABLE logic)
      return cb();
    },

    /**
     * @Issue(
     *   "Add operation for creating the index and its mappings based on model
     *   definition"
     *   type="feature"
     *   priority="normal"
     *   labels="api compliance"
     * )
     */
    define: function (connection, collection, definition, cb) {
			// Add in logic here to create a collection (e.g. CREATE TABLE logic)
      return cb();
    },

    /**
     *
     * REQUIRED method if integrating with a schemaful
     * (SQL-ish) database.
     *
     */
    drop: function (connection, collection, relations, cb) {
			// Add in logic here to delete a collection (e.g. DROP TABLE logic)
			return cb();
    },

    /**
     *
     * REQUIRED method if users expect to call Model.find(), Model.findOne(),
     * or related.
     *
     * You should implement this method to respond with an array of instances.
     * Waterline core will take care of supporting all the other different
     * find methods/usages.
     *
     */
    find: function (connectionName, collectionName, options, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      // We translate the criteria into term filters. For this to work, the
      // mapping for the model needs to define these fields as 'not_analyzed' fields.
      // Otherwise, the 'search' function should be used instead.
      /**
       * @Issue(
       *   "Check how we should be passing the primary key"
       *   type="bug"
       *   priority="normal"
       *   labels="api compliance"
       * )
       * @Issue(
       *   "Set fields to 'not_analyzed' by default, allowing to set them explicitly as 'analyzed'"
       *   type="bug"
       *   priority="normal"
       * )
       * @Issue(
       *   "Allow setting a field to be both 'analyzed' and 'not_analyzed' using
       *   'multi_field' field type"
       *   type="bug"
       *   priority="normal"
       * )
       * @Issue(
       *   "Throw an error if a field included in the criteria is not defined as
       *   'not_analyzed'"
       *   type="bug"
       *   priority="normal"
       * )
       */
      var body = {
        query: {
          bool: {
            filter: []
          }
        }
      };

      for (field in options.where) {
        var term = { term: {} };
        term.term[field] = options.where[field];
        body.query.bool.filter.push(term);
      }

      client.search({
        index: indexName,
        type:  typeName,
        body:  body
      }, function(err, res) {
        if (err) return cb(err);

        /**
         * @Issue(
         *   "Check whether we should be returning the an object that
         *   corresponds to the model instead of simply the values"
         *   type="bug"
         *   priority="normal"
         *   labels="api compliance"
         * )
         */
        getHits(res, function(err, results) {
          if (err) return cb(err);
          return cb(null, results);
        });
      });
    },

    /**
     * @Issue(
     *   "Support auto-incremental primary key generation"
     *   type="improvement"
     *   priority="low"
     * )
     * @Issue(
     *   "Support passing on a custom primary key"
     *   type="improvement"
     *   priority="low"
     * )
     * @Issue(
     *   "Store the createdAt and updatedAt fields to Elastic Search compatible
     *   date formats"
     *   type="improvement"
     *   priority="low"
     * )
     */
    create: function (connectionName, collectionName, values, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      // Restrict values of sub-attributes (for 'json' attributes) according to
      // the attribute's definition.
      var attributes       = getAttributes(connections, connectionName, collectionName);
      var restrictedValues = restrictAttributes(attributes, values);

      client.index({
        index: indexName,
        type:  typeName,
        body:  restrictedValues
      }, function(err, res) {
        if (err) return cb(err);

        /**
         * @Issue(
         *   "Check whether we should be returning the values, with or without
         *   the newly created primary key"
         *   type="bug"
         *   priority="normal"
         *   labels="api compliance"
         * )
         */
        return cb(null, res);
      });
    },

    update: function (connectionName, collectionName, options, values, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      // Restrict values of sub-attributes (for 'json' attributes) according to
      // the attribute's definition.
      var attributes       = getAttributes(connections, connectionName, collectionName);
      var restrictedValues = restrictAttributes(attributes, values);

      /**
       * @Issue(
       *   "Support updating multiple records based on waterline criteria"
       *   type="improvement"
       *   priority="normal"
       *   labels="api compliance"
       * )
       */
      if (options.where.primaryKey == undefined) {
        return cb(new Error('You must specify the primary key of the record you wish to delete.'));
      }

      client.update({
        index: indexName,
        type:  typeName,
        id:    options.where.primaryKey,
        body:  {
          doc: restrictedValues
        }
      }, function(err, res) {
        if (err) return cb(err);
        return cb(null, res);
      });
    },

    destroy: function (connectionName, collectionName, options, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      /**
       * @Issue(
       *   "Support deleting multiple records based on waterline criteria"
       *   type="improvement"
       *   priority="normal"
       *   labels="api compliance"
       * )
       */
      if (options.where.primaryKey == undefined) {
        return cb(new Error('You must specify the primary key of the record you wish to delete.'));
      }

      client.delete({
        index: indexName,
        type:  typeName,
        id:    options.where.primaryKey
      }, function(err, res) {
        if (err) return cb(err);
        return cb();
      });
    },

    /**
     * Get an individual record by it's id.
     */
    get: function (connectionName, collectionName, primaryKey, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      client.get({
        index: indexName,
        type:  typeName,
        id:    primaryKey
      }, function(err, res) {
        if (err) return cb(err);

        /**
         * @Issue(
         *   "Return the record instead of the Elastic Search response"
         *   type="bug"
         *   priority="normal"
         *   labels="api compliance"
         * )
         */
        return cb(null, res);
      });
    },

    delete: function (connectionName, collectionName, primaryKey, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      if (primaryKey == undefined) {
        return cb(new Error('You must specify the primary key of the record you wish to delete.'));
      }

      client.delete({
        index: indexName,
        type:  typeName,
        id:    primaryKey
      }, function(err, res) {
        if (err) return cb(err);
        return cb();
      });
    }

    /*

    // Custom methods defined here will be available on all models
    // which are hooked up to this adapter:
    //
    // e.g.:
    //
    foo: function (connection, collection, options, cb) {
      return cb(null,"ok");
    },
    bar: function (connection, collection, options, cb) {
      if (!options.jello) return cb("Failure!");
      else return cb();
      destroy: function (connection, collection, options, values, cb) {
       return cb();
     }

    // So if you have three models:
    // Tiger, Sparrow, and User
    // 2 of which (Tiger and Sparrow) implement this custom adapter,
    // then you'll be able to access:
    //
    // Tiger.foo(...)
    // Tiger.bar(...)
    // Sparrow.foo(...)
    // Sparrow.bar(...)


    // Example success usage:
    //
    // (notice how the first argument goes away:)
    Tiger.foo({}, function (err, result) {
      if (err) return console.error(err);
      else console.log(result);

      // outputs: ok
    });

    // Example error usage:
    //
    // (notice how the first argument goes away:)
    Sparrow.bar({test: 'yes'}, function (err, result){
      if (err) console.error(err);
      else console.log(result);

      // outputs: Failure!
    })




    */




  };


  // Expose adapter definition
  return adapter;

})();

