var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var PostGisUtils = require('../postgis-utils');
var DataProvider = require('./DataProvider');

var DbWriteListener = DataProvider.Listener.extend({
    initialize : function(options) {
        this.options = options || {};
        this.index = {};
        this._utils = new PostGisUtils(this.options);
    },
    onBegin : function() {
        var that = this;
        return this._utils.newConnection(that.options)//
        .then(function(client) {
            that.client = client;
            var promise = Mosaic.P();
            if (that.options.rebuildDb) {
                var initSql = that._getCreateStatement();
                promise = promise.then(function(query) {
                    return that._utils.runQuery(that.client, initSql);
                });
                return promise;
            }
            return promise.then(function() {
                return client;
            });
        });
    },
    onEnd : function() {
        var that = this;
        return Mosaic.P().then(function(result) {
            return that._rebuildIndexes();
        }) //
        .then(function() {
            if (that.client) {
                that.client.end();
                delete that.client;
            }
        }, function() {
            if (that.client) {
                that.client.end();
                delete that.client;
            }
        });
    },
    onDatasetEntity : function(dataset, entity) {
        this._counter = this._counter || 0;
        this._counter++;
        var that = this;
        return Mosaic.P.then(function() {
            return that._transformToGeoJson(dataset, entity);
        }).then(function(obj) {
            if (!obj)
                return ;
            var sql = that._utils.toPostGisSql(obj, that.options);
            return that._utils.runQuery(that.client, sql);
        })
    },
    _getCreateStatement : function() {
        return this._utils.generateTableCreationSQL(this.options);
    },
    _rebuildIndexes : function() {
        var promise = Mosaic.P();
        var that = this;
        if (that.options.rebuildDb) {
            var indexesSql = that._utils.//
            generateTableIndexesSQL(that.options);
            var viewsSql = that._utils.//
            generateTableViewsSQL(that.options);
            promise = that.utils.//
            runQuery(that.client, indexesSql, viewsSql);
        }
        return promise.then(function() {
            return result;
        })
    }
});

module.exports = DbWriteListener;
