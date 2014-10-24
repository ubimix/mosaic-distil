var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var Path = require('path');
var FS = require('fs');
var DataProvider = require('./DataProvider');
var jsdom = require('jsdom').jsdom;
var tj = require('togeojson');

var KmlDataProvider = DataProvider.extend({
    loadDatasets : function() {
        var dataSets = this.options.dataSets || [];
        return Mosaic.P(dataSets);
    },
    loadDatasetEntities : function(dataset, callback) {
        var dataFolder = this._getDataFolder();
        var fileName = Path.join(dataFolder, dataset.path);
        return this._downloadDataSet(dataset, fileName).then(
                function() {
                    return Mosaic.P.ninvoke(FS, 'readFile', fileName, 'UTF-8')
                        .then(function(str) {
                            var doc = jsdom(str);
                            var result = tj.kml(doc, { styles: true });
                            var list = _.isArray(result) //
                            ? result : _.isArray(result.features) //
                            ? result.features : [ result ];
                            _.each(list, callback);
                        })
                })
    },
});

module.exports = KmlDataProvider;
