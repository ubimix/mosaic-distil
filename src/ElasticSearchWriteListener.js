var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var ElasticSearch = require('elasticsearch');
var DataProvider = require('./DataProvider');

var ElasticSearchWriteListener = DataProvider.Listener.extend({

    // ----------------------------------------------------------------------

    _getEntityType : function(dataset, entity) {
        if (entity.properties && entity.properties.type)
            return entity.properties.type;
        // FIXME:
        return 'mytype';
    },
    _getEntityId : function(dataset, entity) {
        var id = entity.id = entity.id || _.uniqueId('id-');
        return id;
    },

    _getMapping : function() {
        return this.options.mapping || undefined;
    },
    // ----------------------------------------------------------------------

    initialize : function(options) {
        this.options = options || {};
        this.index = {};
        this._client = new ElasticSearch.Client(this.options);
    },
    _openIndex : function(rebuild) {
        var that = this;
        var indexName = that.options.indexName;
        return that._client.indices.exists({
            index : indexName
        }).then(function(exists) {
            if (exists && rebuild) {
                return that._client.indices['delete']({
                    index : indexName
                }).then(function() {
                    return false;
                });
            }
            return exists;
        }).then(function(exists) {
            if (!exists || rebuild) {
                return that._client.indices.create({
                    index : indexName
                })
            }
        }).then(function() {
            // console.log('Mapping', that._getMapping());
            var mapping = that._getMapping();
            if (mapping)
                return that._client.indices.putMapping(mapping);
            return Mosaic.P();
        });
    },
    onBegin : function() {
        var that = this;
        that._batch = [];
        return that._openIndex(that.options.recreateIndex);
    },
    onEnd : function() {
        var that = this;
        return that._flushBatch();
    },
    onDatasetEntity : function(dataset, entity) {
        var that = this;
        var indexName = that.options.indexName;
        var type = that._getEntityType(dataset, entity);
        return Mosaic.P.then(function() {
            entity.id = that._getEntityId(dataset, entity);
            var command = {
                index : {
                    _index : indexName,
                    _type : type,
                    _id : entity.id
                }
            };
            that._batch.push(command);
            that._batch.push(entity);
            if (that._shouldFlushBatch(that._batch)) {
                return that._flushBatch();
            }
        });
    },
    _shouldFlushBatch : function(batch) {
        // Batches contain two lines per record: one line - a command; the
        // second line - data to process
        return (batch.length % 200) === 0;
    },
    _flushBatch : function() {
        var that = this;
        return Mosaic.P.then(function() {
            var batch = that._batch;
            that._batch = [];
            if (batch.length > 0) {
                return that._client.bulk({
                    body : batch
                }).then(function(err, resp) {
                    console.log('bulk indexing... #', batch.length, ' error:', err.errors);
                });
            }
        });
    }
});

module.exports = ElasticSearchWriteListener;