var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var Utils = require('../fetch-utils');
var Path = require('path');
var DataProvider = require('./DataProvider');

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
});

module.exports = CsvDataProvider;