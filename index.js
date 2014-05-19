var _ = require('underscore');
var P = require('q');
var Utils = require('./fetch-utils');
var Path = require('path');
var FS = require('fs');
var Url = require('url');
var PostGisUtils = require('./postgis-utils');

function Class(options) {
    this.initialize(options);
}
_.extend(Class.prototype, {
    initialize : function(options) {
        this.options = options || {};
    }
});
_.extend(Class, {
    extend : function(options) {
        function F() {
            F.Parent.apply(this, arguments);
        }
        F.Parent = this;
        _.extend(F, this);
        _.extend(F.prototype, this.prototype, options);
        return F;
    }
})

var DataProvider = Class.extend({
    handleAll : function(listener) {
        var that = this;
        var dataSets;
        return that.loadDatasets().then(function(sets) {
            dataSets = sets;
            return listener.onBegin(dataSets);
        }).then(function() {
            return that._handleDatasets(_.map(dataSets, function(dataset) {
                return listener.onBeginDataset(dataset).then(function() {
                    var promise = P();
                    return that.loadDatasetEntities(dataset, function(entity) {
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
        return P.all(list);
    },

    /* ---------------------------------- */
    /* "Abstract" methods to overload in subclasses. */

    /** Returns a promise for a list of datasets */
    loadDatasets : function() {
        return P([]);
    },
    /** Loads dataset entities and notifies them to the specified callback */
    loadDatasetEntities : function(dataset, callback) {
        return P();
    },

    /* ---------------------------------- */
    /* Utility methods used in subclasses. */

    _getDataFolder : function() {
        return this.options.dataFolder;
    },
    _downloadDataSet : function(dataset, fileName) {
        var url = dataset.url;
        if (!url || url == '') {
            return P(false);
        }
        if (FS.existsSync(fileName) && !this.options.forceDownload) {
            return P(true);
        }
        return Utils.download(fileName, url).then(function(doc) {
            return true;
        })
    }

});

var Listener = DataProvider.Listener = Class.extend({
    onError : function(err) {
        throw err;
    },
    onBegin : function(datasets) {
        return P();
    },
    onEnd : function(datasets) {
        return P();
    },
    onBeginDataset : function(dataset) {
        return P();
    },
    onEndDataset : function(dataset) {
        return P();
    },
    onDatasetEntity : function(dataset, entity) {
        return this._transformToGeoJson(dataset, entity);
    },

    _transformToGeoJson : function(dataset, obj) {
        if (_.isFunction(dataset.transform)) {
            return dataset.transform(obj);
        }
        return P(obj);
    }
})

var WriteListener = Listener.extend({
    initialize : function(options) {
        this.options = options || {};
        this.index = {};
    },
    onBeginDataset : function(dataset) {
        var info = this.index[dataset.path] = {
            counter : 0
        };
        info.fileName = Path.join(this.options.dataFolder, dataset.path);
        info.destFile = this._getDestFile(info);
        var dir = Path.dirname(info.destFile);
        var promise = P();
        if (!FS.existsSync(dir)) {
            promise = P.ninvoke(FS, 'mkdir', dir);
        }
        return promise.then(function() {
            info.output = FS.createWriteStream(info.destFile, {
                flags : 'w',
                encoding : 'UTF-8'
            });
            return P.ninvoke(info.output, 'write', '[\n', 'UTF-8');
        })
    },
    onEndDataset : function(dataset) {
        var info = this.index[dataset.path];
        delete this.index[dataset.path];
        return info && info.output ? P
                .ninvoke(info.output, 'end', ']', 'UTF-8') : P();
    },
    onDatasetEntity : function(dataset, entity) {
        var that = this;
        return P().then(function() {
            return that._transformToGeoJson(dataset, entity);
        }).then(function(obj) {
            var str = JSON.stringify(obj, null, 2);
            var info = that.index[dataset.path];
            if (info.counter > 0) {
                str = ',\n' + str;
            }
            info.counter++;
            return P.ninvoke(info.output, 'write', str, 'UTF-8');
        });
    },
    _setExtension : function(fileName, newExt) {
        return Path.join(Path.dirname(fileName), Path.basename(fileName, Path
                .extname(fileName))
                + newExt);
    },
    _getDestFile : function(info) {
        return this._setExtension(info.fileName, '.json');
    }

});

var DbWriteListener = Listener.extend({
    initialize : function(options) {
        this.options = options || {};
        this.index = {};
    },
    onBegin : function() {
        var that = this;
        return PostGisUtils.newConnection(that.options).then(
                function(client) {
                    that.client = client;
                    var promise = P();
                    if (that.options.rebuildDb) {
                        var initSql = PostGisUtils
                                .generateTableCreationSQL(that.options);
                        promise = promise.then(function(query) {
                            return PostGisUtils.runQuery(that.client, initSql);
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
        return P().then(
                function(result) {
                    var promise = P();
                    if (that.options.rebuildDb) {
                        var indexesSql = PostGisUtils
                                .generateTableIndexesSQL(that.options);
                        var viewsSql = PostGisUtils
                                .generateTableViewsSQL(that.options);
                        promise = PostGisUtils.runQuery(that.client,
                                indexesSql, viewsSql);
                    }
                    return promise.then(function() {
                        return result;
                    })
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
        return P().then(function() {
            return that._transformToGeoJson(dataset, entity);
        }).then(function(obj) {
            var sql = PostGisUtils.toPostGisSql(obj, that.options);
            return PostGisUtils.runQuery(that.client, sql);
        })
    },
})

var LogListener = Listener.extend({
    initialize : function(options) {
        this.listener = options.listener;
        this.index = {};
    },
    onBegin : function(datasets) {
        console.log('Begin');
        return this.listener.onBegin(datasets);
    },
    onEnd : function(datasets) {
        return this.listener.onEnd(datasets).then(function() {
            console.log('End');
        }, function(err) {
            console.log('End', err);
        });
    },
    onBeginDataset : function(dataset) {
        var info = this.index[dataset.path] = {
            counter : 0
        };
        console.log('Begin [' + dataset.path + ']');
        return this.listener.onBeginDataset(dataset);
    },
    onEndDataset : function(dataset) {
        var info = this.index[dataset.path];
        delete this.index[dataset.path];
        return this.listener.onEndDataset(dataset).then(
                function() {
                    console.log('End [' + dataset.path + '] - ' + info.counter
                            + ' records.');
                });
    },
    onDatasetEntity : function(dataset, entity) {
        var info = this.index[dataset.path];
        info.counter++;
        if ((info.counter % 1000) == 0) {
            console.log(' * [' + dataset.path + '] : ' + info.counter);
        }
        return this.listener.onDatasetEntity(dataset, entity);
    },

})

var CsvDataProvider = DataProvider.extend({
    loadDatasets : function() {
        var dataSets = this.options.dataSets || [];
        return P(dataSets);
    },
    loadDatasetEntities : function(dataset, callback) {
        var dataFolder = this._getDataFolder();
        var fileName = Path.join(dataFolder, dataset.path);
        return this._downloadDataSet(dataset, fileName).then(function() {
            var csvOptions = dataset.csvOptions || {};
            return Utils.readCsv(fileName, callback, csvOptions);
        })
    },
})

var JsonDataProvider = DataProvider.extend({
    loadDatasets : function() {
        var dataSets = this.options.dataSets || [];
        return P(dataSets);
    },
    loadDatasetEntities : function(dataset, callback) {
        var dataFolder = this._getDataFolder();
        var fileName = Path.join(dataFolder, dataset.path);
        return this._downloadDataSet(dataset, fileName).then(
                function() {
                    return P.ninvoke(FS, 'readFile', fileName, 'UTF-8').then(
                            function(str) {
                                var result = JSON.parse(str);
                                if (_.isArray(result)) {
                                    _.each(result, callback);
                                } else {
                                    callback(result);
                                }
                            })
                })
    },
})

module.exports = {
    Class : Class,
    DataProvider : DataProvider,
    WriteListener : WriteListener,
    DbWriteListener : DbWriteListener,
    LogListener : LogListener,
    CsvDataProvider : CsvDataProvider,
    JsonDataProvider : JsonDataProvider
}
