/**
 * @Issue(
 *   "Consider breaking the adapter into modules e.g. database"
 *   type="task"
 *   priority="low"
 *   labels="refactoring, structure, readability"
 * )
 */

/**
 * Module Dependencies
 */

var _          = require('lodash');
var async      = require('async');
var Connection = require('./connection');

// Set the limit for concurrent 'async' operations. Prefer the value defined in
// the application configuration, if it exists.
var asyncLimit = _(sails.config).get('custom.async.limit') || 100;

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

// Helper function for getting the name of the primary key field for the model.
var getPrimaryKeyField = function(connections, connectionName, collectionName) {
  return connections[connectionName].collections[collectionName].primaryKey;
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
    return cb(null, results);
  }

  async.eachLimit(esResponse.hits.hits, asyncLimit, function(e, c) {
    results.push(hitToModel(e));
    c();
  }, function(err) {
    if (err) return cb(err);
    return cb(null, results);
  });
};

// Helper function for getting documents as models from an Elastic Search
// response that provides a list of 'docs', such as a response to an 'mget'
// request.
var getFound = function(esResponse, cb) {
  var results = [];

  async.eachLimit(esResponse.docs, asyncLimit, function(e, c) {
    if (e.found) {
      results.push(hitToModel(e));
      c();
    }
    else {
      results.push(false);
      c();
    }
  }, function(err) {
    if (err) return cb(err);
    return cb(null, results);
  });
};

// Convert an Elastic Search "hit" into a json object that corresponds to the
// model at hand.
/**
 * @Issue(
 *   "Check whether we should be returning an actual model object"
 *   type="task"
 *   priority="normal"
 *   labels="api compliance"
 * )
 */
var hitToModel = function(hit) {
  var result = hit._source;
  result._id = hit._id;
  return result;
}

// Make a Term or Terms query.
// If we're looking for a single value, we make a term query.
// If we're looking for multiple values, we make a terms query.
var smartTermOrTermsQuery = function(field, value) {
  var termFilter = {};

  // If we want to make an IN query, which means that the comparison
  // value must be an array, we create a 'terms' filter.
  if (Array.isArray(value)) {
    termFilter.terms = {};
    termFilter.terms[field] = value;
  } else {
    // Otherwise, we create a 'term' filter.
    termFilter.term = {};
    termFilter.term[field] = value;
  }

  return termFilter;
}

// Restrict sub-attributes in attributes of type 'json' to the ones
// defined by the 'restrictAttributes' and 'skipAttributes' properties.
var restrictAttributes = function(attributes, values) {
  Object.keys(attributes).forEach(function(attribute) {
    if (attributes[attribute].type != undefined && attributes[attribute].type == 'json') {
      // Remove attributes that are not included in the restrictAttributes
      // property, if defined.
      if (attributes[attribute].restrictAttributes && values[attribute]) {
        if (Array.isArray(values[attribute])) {
          values[attribute].forEach(function(e) {
            restrictObjectAttributes(attributes[attribute].restrictAttributes, e);
          });
        } else {
          restrictObjectAttributes(attributes[attribute].restrictAttributes, values[attribute]);
        }
      }

      function restrictObjectAttributes(restrictedFields, object) {
        Object.keys(object).forEach(function(e) {
          if (_.indexOf(restrictedFields, e) == -1) {
            delete object[e];
          }
        });
      }

      // Remove attributes included in the skipAttributes property, if defined.
      /**
       * @Issue(
       *   "Properly filter json array as well, like when restricting attributes"
       *   type="bug"
       *   priority="normal"
       * )
       */
      if (attributes[attribute].skipAttributes && values[attribute]) {
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
     *   "Check what is the value passed in the 'definition' argument, and
     *   potentially use it"
     *   type="bug"
     *   priority="low"
     *   labels="api compliance"
     * )
     * @Issue(
     *   "Define this function using the 'createIndex' and the 'putMapping'
     *   functions."
     *   type="bug"
     *   priority="normal"
     *   labels="api compliance"
     * )
     */
    define: function (connectionName, collectionName, definition, cb, index) {
    },

    /**
     * @Issue(
     *   "Allow defining the index settings as well in the model"
     *   type="improvement"
     *   priority="normal"
     * )
     */
    createIndex: function (connectionName, collectionName, settings, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);

      // Check if the index exists first.
      client.indices.exists({
        index: indexName
      }, function(err, res) {
        if (err) return cb(err);

        if (res == true) {
          return cb(new Error('Index already exists.'));
        }

        var body = {
          index : indexName
        };

        if (settings != undefined) {
          body.settings = settings;
        }

        client.indices.create(body, function(err, res) {
          if (err) return cb(err);
          return cb(null, res);
        });
      });
    },

    /**
     * @Issue(
     *   "Add mapping for createdAt and updatedAt fields, when autogenerated"
     *   type="bug"
     *   priority="low"
     * )
     */
    putMapping: function (connectionName, collectionName, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      // Load the model's attributes so that we can get the fields' mappings.
      var attributes = getAttributes(connections, connectionName, collectionName);

      var body = {
        properties: {}
      };

      // For each model's attribute that has the 'mapping' property defined, add
      // the containing mapping to the request's body.
      async.each(Object.keys(attributes), function(e, c) {
        if (attributes[e].mapping != undefined) {
          body.properties[e] = attributes[e].mapping;
        }
        c();
      }, function(err) {
        if (err) return cb(err);

        // If there was no mappings defined, no need to make a request.
        if (_.isEmpty(body.properties)) {
          return cb(null, 'No mapping defined for the ' + '"' + indexName + '" index.');
        }

        // After all mappings have been added to the request's body, make the
        // request.
        client.indices.putMapping({
          index: indexName,
          type:  typeName,
          body:  body
        }, function(err, res) {
          if (err) return cb(err);
          return cb(null, res);
        });
      });
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
        query: {}
      };

      // Issue a 'match_all' query if there are no 'where' criteria specified.
      if (options.where) {
        body.query.bool = {
          filter: []
        }
      } else {
        body.query['match_all'] = {};
      };

      // Add term filters based on the given criteria.
      for (field in options.where) {
        var fieldParts      = field.split('.');
        var fieldPartsCount = fieldParts.length;

        // If the field is not nested, add a term filter.
        if (fieldPartsCount == 1) {
          var termFilter = smartTermOrTermsQuery(field, options.where[field]);
          body.query.bool.filter.push(termFilter);
        } else {
          // If the field is nested, construct a nested query with a term
          // filter at its last level. It can possibly be multi-level.
          var termFilter = smartTermOrTermsQuery(field, options.where[field]);
          fieldParts.pop();

          var nestedQuery = {};
          fieldParts.reduce(function(result, value, key, collection) {
            var path = result[0] ? result[0] + '.' + value : value;
            result[1].nested = { path  : path };

            if (key < fieldPartsCount - 2) {
              result[1].nested.query = {};
              return [path, result[1].nested.query, termFilter];
            }

            // If we have reached the point where 'key == fieldPartsCount - 2',
            // then we are at the last iteration.
            result[1].nested.query = {
              bool: {
                filter: termFilter
              }
            };
            return;
          }, ['', nestedQuery, termFilter]);
          body.query.bool.filter.push(nestedQuery);
        }
      }

      // Add offset and limit to the query, if requested.
      if (options.skip != undefined) {
        body.from = options.skip;
      }
      if (options.limit != undefined) {
        body.size = options.limit;
      }

      // Add sorting, if requested.
      if (options.sort != undefined) {
        var sortFields = Object.keys(options.sort);
        if (sortFields.length) {
          var sorts = [];

          // We could have multiple sort criteria - add each one of them in the
          // order provided.
          sortFields.forEach(function(sortField) {
            var sortQuery = {};
            sortQuery[sortField] = {
              order: options.sort[sortField] == 1 ? 'asc' : 'desc'
            }
            sorts.push(sortQuery);
          });

          body.sort = sorts;
        }
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

        // If we have no error, the response should contain the id of the
        // created record. Get the record and return it.
        /**
         * @Issue(
         *   "Consider constructing the record from the given values and the
         *   id returned in the response"
         *   type="improvement"
         *   priority="low"
         *   labels="performance"
         * )
         */
        client.get({
          index: indexName,
          type:  typeName,
          id:    res._id
        }, function(err, res) {
          if (err) return cb(err);
          return cb(null, hitToModel(res));
        });
      });
    },

    update: function (connectionName, collectionName, options, values, cb, index) {
      var client          = getClient(connections, connectionName);
      var indexName       = getIndex(connections, connectionName, collectionName, index);
      var typeName        = getType(connections, connectionName, collectionName);
      var primaryKeyField = getPrimaryKeyField(connections, connectionName, collectionName);

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
      if (options.where[primaryKeyField] == undefined) {
        return cb(new Error('You must specify the primary key of the record you wish to delete.'));
      }

      client.update({
        index: indexName,
        type:  typeName,
        id:    options.where[primaryKeyField],
        body:  {
          doc: restrictedValues
        }
      }, function(err, res) {
        if (err) return cb(err);

        // If we have no error, the response should contain the id of the
        // updated record. Get the record and return it.
        client.get({
          index: indexName,
          type:  typeName,
          id:    res._id
        }, function(err, res) {
          if (err) return cb(err);
          return cb(null, hitToModel(res));
        });
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
        return cb(null, hitToModel(res));
      });
    },

    /**
     * Get multiple records by their ids.
     */
    mget: function (connectionName, collectionName, primaryKeys, cb, index) {
      var client    = getClient(connections, connectionName);
      var indexName = getIndex(connections, connectionName, collectionName, index);
      var typeName  = getType(connections, connectionName, collectionName);

      client.mget({
        index: indexName,
        type:  typeName,
        body: {
          ids: primaryKeys
        }
      }, function(err, res) {
        if (err) return cb(err);

        getFound(res, function(err, results) {
          if (err) return cb(err);
          return cb(null, results);
        });
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

