var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var Utils = require('../fetch-utils');
var Path = require('path');
var FS = require('fs');
var Shapefile = require('shapefile');
var DataProvider = require('./DataProvider');

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
                    return Mosaic.P.ninvoke(FS, 'readdir', dir)//
                    .then(function(list) {
                        var file = _.find(list, function(name) {
                            return Path.extname(name) == '.shp';
                        });
                    })
                    return Path.join(dir, file);
                });
            });
        }).then(function(file) {
            var reader = Shapefile.reader(file, {
                encoding : undefined,
                'ignore-properties' : false
            });
            return Mosaic.P.ninvoke(reader, 'readHeader')//
            .then(function(header) {
            }).then(function readRecord() {
                return Mosaic.P.ninvoke(reader, 'readRecord')//
                .then(function(record) {
                    if (record === Shapefile.end) {
                        return;
                    }
                    callback(record);
                    return readRecord();
                });
            }).then(function() {
                return Mosaic.P.ninvoke(reader, 'close');
            });
        })
    },
});
module.exports = ShapeDataProvider;