var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var Utils = require('./fetch-utils');
var Path = require('path');
var FS = require('fs');
var Url = require('url');
var PostGisUtils = require('./postgis-utils');
var Shapefile = require('shapefile');

var DataProvider = Mosaic.Class.extend({
    initialize : function(options){
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

var Listener = DataProvider.Listener = Mosaic.Class.extend({
    onError : function(err) {
        throw err;
    },
    onBegin : function(datasets) {
        return Mosaic.P();
    },
    onEnd : function(datasets) {
        return Mosaic.P();
    },
    onBeginDataset : function(dataset) {
        return Mosaic.P();
    },
    onEndDataset : function(dataset) {
        return Mosaic.P();
    },
    onDatasetEntity : function(dataset, entity) {
        return this._transformToGeoJson(dataset, entity);
    },

    _transformToGeoJson : function(dataset, obj) {
        if (_.isFunction(dataset.transform)) {
            return dataset.transform(obj);
        }
        return Mosaic.P(obj);
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
        var promise = Mosaic.P();
        if (!FS.existsSync(dir)) {
            promise = Mosaic.P.ninvoke(FS, 'mkdir', dir);
        }
        return promise.then(function() {
            info.output = FS.createWriteStream(info.destFile, {
                flags : 'w',
                encoding : 'UTF-8'
            });
            return Mosaic.P.ninvoke(info.output, 'write', '[\n', 'UTF-8');
        })
    },
    onEndDataset : function(dataset) {
        var info = this.index[dataset.path];
        delete this.index[dataset.path];
        return info && info.output ? Mosaic.P
                .ninvoke(info.output, 'end', ']', 'UTF-8') : Mosaic.P();
    },
    onDatasetEntity : function(dataset, entity) {
        var that = this;
        return Mosaic.P.then(function() {
            return that._transformToGeoJson(dataset, entity);
        }).then(function(obj) {
            var str = JSON.stringify(obj, null, 2);
            var info = that.index[dataset.path];
            if (info.counter > 0) {
                str = ',\n' + str;
            }
            info.counter++;
            return Mosaic.P.ninvoke(info.output, 'write', str, 'UTF-8');
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

var DbWriteListener = Listener
        .extend({
            initialize : function(options) {
                this.options = options || {};
                this.index = {};
            },
            onBegin : function() {
                var that = this;
                return PostGisUtils.newConnection(that.options).then(
                        function(client) {
                            that.client = client;
                            var promise = Mosaic.P();
                            if (that.options.rebuildDb) {
                                var initSql = that._getCreateStatement();
                                promise = promise.then(function(query) {
                                    return PostGisUtils.runQuery(that.client,
                                            initSql);
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
                    var sql = PostGisUtils.toPostGisSql(obj, that.options);
                    return PostGisUtils.runQuery(that.client, sql);
                })
            },
            _getCreateStatement : function() {
                return PostGisUtils.generateTableCreationSQL(this.options);
            },
            _rebuildIndexes : function() {
                var promise = Mosaic.P();
                var that = this;
                if (that.options.rebuildDb) {
                    var indexesSql = PostGisUtils
                            .generateTableIndexesSQL(that.options);
                    var viewsSql = PostGisUtils
                            .generateTableViewsSQL(that.options);
                    promise = PostGisUtils.runQuery(that.client, indexesSql,
                            viewsSql);
                }
                return promise.then(function() {
                    return result;
                })
            }
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
        return Mosaic.P(dataSets);
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
        return Mosaic.P(dataSets);
    },
    loadDatasetEntities : function(dataset, callback) {
        var dataFolder = this._getDataFolder();
        var fileName = Path.join(dataFolder, dataset.path);
        return this._downloadDataSet(dataset, fileName).then(
                function() {
                    return Mosaic.P.ninvoke(FS, 'readFile', fileName, 'UTF-8').then(
                            function(str) {
                                var result = JSON.parse(str);
                                var list = _.isArray(result) //
                                ? result //
                                : _.isArray(result.features) //
                                ? result.features : [ result ];
                                _.each(list, callback);
                            })
                })
    },
})

var ShapeDataProvider = DataProvider.extend({
    loadDatasets : function() {
        var dataSets = this.options.dataSets || [];
        return Mosaic.P(dataSets);
    },
    loadDatasetEntities : function(dataset, callback) {
        var dataFolder = this._getDataFolder();
        var fileName = Path.join(dataFolder, dataset.path);
        return this._downloadDataSet(dataset, fileName).then(function() {
            var name = Path.basename(fileName);
            var ext = Path.extname(name);
            if (ext != '') {
                name = name.substring(0, name.length - ext.length);
            } else {
                name + '.shp';
            }
            var dir = Path.join(Path.dirname(fileName), name);
            return Utils.checkDir(dir).then(function(exists) {
                return Utils.unzip(fileName, dir).then(function() {
                    return Mosaic.P.ninvoke(FS, 'readdir', dir).then(function(list) {
                        var file = _.find(list, function(name) {
                            return Path.extname(name) == '.shp';
                        })
                        return Path.join(dir, file);
                    });
                })
            }).then(function(file) {
                var reader = Shapefile.reader(file, {
                    encoding : undefined,
                    'ignore-properties' : false
                });
                return Mosaic.P.ninvoke(reader, 'readHeader').then(function(header) {
                }) //
                .then(function readRecord() {
                    return Mosaic.P.ninvoke(reader, 'readRecord') // 
                    .then(function(record) {
                        if (record === Shapefile.end) {
                            return;
                        }
                        callback(record);
                        return readRecord();
                    })
                }).then(function() {
                    return Mosaic.P.ninvoke(reader, 'close');
                })
            })
        })
    },
})

module.exports = Mosaic.Distil = {
    DataProvider : DataProvider,
    WriteListener : WriteListener,
    DbWriteListener : DbWriteListener,
    LogListener : LogListener,
    CsvDataProvider : CsvDataProvider,
    JsonDataProvider : JsonDataProvider,
    ShapeDataProvider : ShapeDataProvider
}
