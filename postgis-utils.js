var PG = require('pg.js');
// or native libpq bindings
// var PG = require('PG').native
var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var FS = require('fs');
// var Terraformer = require('terraformer');
var WKT = require('terraformer-wkt-parser');

function toPostGisWKT(geometry) {
    var wkt = WKT.convert(geometry);
    return 'ST_GeomFromText(\'' + wkt + '\', 4326)';
}

function esc(str) {
    if (!str)
        return 'null';
    return "'" + str.replace(/\'/gim, "''") + "'";
}

function checkParams(params) {
    params = params || {};
    if (!params.table) {
        params.table = 'objects';
    }
    if (!params.type) {
        params.type = 'object';
    }
    return params;
}

function writeText(file, str) {
    return Mosaic.P.ninvoke(FS, 'writeFile', file, str);
}

function readText(file) {
    return Mosaic.P.ninvoke(FS, 'readFile', file, 'utf8');
}

function writeJson(file, obj) {
    var str = JSON.stringify(obj, null, 2);
    return writeText(file, str);
}

function readJson(file) {
    return readText(file).then(function(str) {
        return JSON.parse(str);
    });
}

function PostGisUtils(options) {
    this.options = _.extend({}, options);
    this.options.propertyFields = this.options.propertyFields ||
            [ 'properties' ];
}
PostGisUtils.prototype = {

    options : {
        propertyFields : [ 'properties', 'properties_en', 'properties_es',
                'properties_fr', 'properties_de' ]
    },

    /**
     * Creates a new connection to the DB
     * 
     * @param params.dbname
     */
    newConnection : function(params) {
        var url = params.url;
        var client = new PG.Client(url);
        return Mosaic.P.ninvoke(client, 'connect');
    },

    closeConnection : function(conn) {
        if (_.isFunction(conn.end)) {
            conn.end();
        }
        return Mosaic.P();
    },

    runQuery : function(client) {
        var queries = _.toArray(arguments);
        queries.splice(0, 1);
        var promise = Mosaic.P();
        var results = [];
        _.each(queries, function(sql) {
            promise = promise.then(function(r) {
                results.push(r);
                return Mosaic.P.ninvoke(client, 'query', sql);
            })
        })
        return promise;
    },

    _getPropertyFields : function() {
        return this.options.propertyFields || [ 'properties' ];
    },

    toPostGisSql : function(obj, params) {
        params = checkParams(params);
        var properties = obj.properties || {};
        var geometry = obj.geometry;
        if (geometry && !(_.isArray(geometry.coordinates) && geometry.type)) {
            geometry = null;
        }
        type = properties.type || params.type;
        var fieldNames = [ 'type' ];
        var fieldValues = [ esc(type) ];
        var propertyFields = this._getPropertyFields();
        _.each(propertyFields, function(field) {
            var properties = obj[field];
            fieldNames.push(field);
            fieldValues.push(properties ? esc(JSON.stringify(properties)) +
                    "::json" : 'NULL');
        });
        if (geometry && geometry.coordinates && geometry.coordinates.length &&
                geometry.coordinates[0]) {
            fieldNames.push('geometry');
            fieldValues.push(toPostGisWKT(geometry));
        }
        var sql = 'insert into <%=table%> (<%=names%>) values (<%=values%>);';
        return _.template(sql)(_.extend({}, params, {
            names : fieldNames.join(", "),
            values : fieldValues.join(', ')
        }));
    },

    toPostGisWKT : toPostGisWKT,

    generateTableCreationSQL : function(params) {
        params = checkParams(params);
        var propertyFields = this._getPropertyFields();
        var fields = _.map(propertyFields, function(field) {
            return field + ' json';
        }).join(', ');
        var sql = 'drop table if exists <%=table%> cascade;\n';
        sql += 'create table <%=table%> ' +
                '(id serial primary key, type varchar(255), ' + fields +
                ', geometry geometry, \n' +
                'check (st_srid(geometry) = 4326)\n);\n';
        return _.template(sql)(params);
    },

    generateTableIndexesSQL : function(params) {
        params = checkParams(params);
        return _
                .template(
                        "create index on <%=table%>(type);\n"
                                + "create index on <%=table%>((properties->>'type'));\n\n")
                (params);
    },

    generateTableViewsSQL : function(params) {
        params = checkParams(params);
        var propertyFields = this._getPropertyFields();
        var fields = _.map(propertyFields, function(field) {
            return '<%=table%>.' + field;
        }).join(', ');
        var sql = "create or replace view <%=table%>_webmercator \n" +
                "as select <%=table%>.id, <%=table%>.type, " + fields + ", \n" +
                "<%=table%>.geometry, \n" +
                "st_transform(<%=table%>.geometry, 3857) \n" +
                "as the_geom_webmercator \n" + "from <%=table%>;\n";
        return _.template(sql)(params);
    },

    writeText : writeText,

    readText : readText,

    writeJson : writeJson,

    readJson : readJson
}

_.extend(PostGisUtils, PostGisUtils.prototype);

return module.exports = PostGisUtils;
