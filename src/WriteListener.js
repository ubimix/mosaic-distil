var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var Path = require('path');
var FS = require('fs');
var DataProvider = require('./DataProvider');

var WriteListener = DataProvider.Listener.extend({
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
        return info && info.output ? Mosaic.P.ninvoke(info.output, 'end', ']',
                'UTF-8') : Mosaic.P();
    },
    onDatasetEntity : function(dataset, entity) {
        var that = this;
        return Mosaic.P.then(function() {
            return that._transformToGeoJson(dataset, entity);
        }).then(function(obj) {
            if (!obj)
                return;
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
module.exports = WriteListener;