var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var Utils = require('../fetch-utils');
var FS = require('fs');

var DataProvider = Mosaic.Class.extend({
    initialize : function(options) {
        this.setOptions(options);
    },
    handleAll : function(listener) {
        var that = this;
        var dataSets;
        return that.loadDatasets().then(function(sets) {
            dataSets = sets;
            return listener.onBegin(dataSets);
        }).then(function() {
            return that._handleDatasets(_.map(dataSets, function(dataset) {
                return listener.onBeginDataset(dataset).then(function() {
                    var promise = Mosaic.P();
                    return that.loadDatasetEntities(dataset, function(entity) {
                        if (!entity)
                            return ;
                        promise = promise.then(function() {
                            return listener.onDatasetEntity(dataset, entity);
                        });
                    }).then(function() {
                        return promise;
                    });
                }).then(function(result) {
                    listener.onEndDataset(dataset);
                    return result;
                }, function(err) {
                    listener.onEndDataset(dataset);
                    return listener.onError(err);
                })
            }));
        }).then(function(result) {
            listener.onEnd(dataSets);
            return result;
        }, function(err) {
            listener.onEnd(dataSets);
            return listener.onError(err);
        });
    },

    _handleDatasets : function(list) {
        return Mosaic.P.all(list);
    },

    /* ---------------------------------- */
    /* "Abstract" methods to overload in subclasses. */

    /** Returns a promise for a list of datasets */
    loadDatasets : function() {
        return Mosaic.P([]);
    },
    /** Loads dataset entities and notifies them to the specified callback */
    loadDatasetEntities : function(dataset, callback) {
        return Mosaic.P();
    },

    /* ---------------------------------- */
    /* Utility methods used in subclasses. */

    _getDataFolder : function() {
        return this.options.dataFolder;
    },
    _downloadDataSet : function(dataset, fileName) {
        var url = dataset.url;
        if (!url || url == '') {
            return Mosaic.P(false);
        }
        if (FS.existsSync(fileName) && !this.options.forceDownload) {
            return Mosaic.P(true);
        }
        return Utils.download(fileName, url).then(function(doc) {
            return true;
        })
    }

});

DataProvider.Listener = Mosaic.Class.extend({
    onError : function(err) {
        throw err;
    },
    onBegin : function(datasets) {
        return Mosaic.P();
    },
    onEnd : function(datasets) {
        return Mosaic.P.then(function() {
        });
    },
    onBeginDataset : function(dataset) {
        return Mosaic.P.then(function() {
        });
    },
    onEndDataset : function(dataset) {
        return Mosaic.P.then(function() {
        });
    },
    onDatasetEntity : function(dataset, entity) {
        return this._transformToGeoJson(dataset, entity);
    },

    _transformToGeoJson : function(dataset, obj) {
        return Mosaic.P.then(function() {
            if (_.isFunction(dataset.transform)) {
                return dataset.transform(obj);
            }
            return obj;
        });
    }
})

module.exports = DataProvider;
