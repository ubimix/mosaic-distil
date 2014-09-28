var Mosaic = require('mosaic-commons');
var _ = require('underscore');
var DataProvider = require('./DataProvider');

var LogListener = DataProvider.Listener.extend({
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

});

module.exports = LogListener;