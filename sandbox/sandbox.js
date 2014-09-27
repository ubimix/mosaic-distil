var _ = require('underscore');
var Mosaic = require('mosaic-commons');
var MosaicDistil = require('../');
var Utils = require('../transform-utils');
var dataFolder = './tmp';

var listener = new MosaicDistil.WriteListener({
    dataFolder : dataFolder
});
listener = new MosaicDistil.LogListener({
    listener : listener,
});

var dataSets = [];
dataSets.push(D01_CountriesConfig());
var dataProvider = new MosaicDistil.ShapeDataProvider({
    dataSets : dataSets,
    dataFolder : dataFolder,
    forceDownload : false
})
return dataProvider.handleAll(listener).then(null, function(err) {
    console.log(' * >>> ', err.stack);
}).done();

function D01_CountriesConfig() {
    return Utils
            .newDataSet({
                "path" : "ne_110m_admin_0_countries.zip",
                "url" : "http://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries.zip",
                transform : function(obj) {
                    console.log(JSON.stringify(obj, null, 2));
                    return obj;
                }
            });
}
