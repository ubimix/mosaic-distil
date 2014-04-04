var _ = require('underscore');
var Utils = require('./transform-utils');
var Q = require('q');

var exported = module.exports = [];

// 25 - La carte des hôtels classés en Île-de-France
exported.push(CciConfig());

/* ------------------------------------------------------------ */

function CciConfig() {
    return Utils.newDataSet({
        "path" : "commerces.csv",
        "url" : "",
        csvOptions : {
            delim : ','
        },
        transform : function(item) {
            var that = this;
            var result = {
                type : 'Feature',
                properties : _.extend({
                    type : 'Commerce'
                }, that._toProperties(item, {}))
            };
            return result;
        }
    });
}

/* ------------------------------------------------------------ */

<<<<<<< HEAD
// 08 - Commissariat de Police
function D08_Police() {
    return Utils
            .newDataSet({
                "path" : "cartographie-des-emplacements-des-commissariats-a-paris-et-petite-couronne.csv",
                "url" : "http://data.iledefrance.fr/explore/dataset/cartographie-des-emplacements-des-commissariats-a-paris-et-petite-couronne/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'Police'
                        }, this._toProperties(obj, {
                            exclude : [ 'coordinates' ],
                            convert : {
                                'name' : 'label'
                            }
                        })),
                        geometry : this._toGeometryPoint(obj.coordinates)
                    }
                }
            });
}

/* ------------------------------------------------------------ */

// 09 - Velib
function D09_Velib() {
    return Utils
            .newDataSet({
                "path" : "velib_a_paris_et_communes_limitrophes.csv",
                "url" : "http://data.iledefrance.fr/explore/dataset/velib_a_paris_et_communes_limitrophes/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'Velib'
                        }, this._toProperties(obj, {
                            exclude : [ 'latitude', 'longitude', 'wgs84' ],
                            convert : {
                                'name' : 'label'
                            }
                        })),
                        geometry : this._toGeometryPoint(obj.wgs84)
                    }
                }
            });
}

/* ------------------------------------------------------------ */

// 10 - Monuments
function D10_Monuments() {
    return Utils
            .newDataSet({
                "path" : "monuments-inscrits-ou-classes-dile-de-france.csv",
                "url" : "http://data.iledefrance.fr/explore/dataset/monuments-inscrits-ou-classes-dile-de-france/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'Monument'
                        }, this._toProperties(obj, {
                            exclude : [ 'geo_shape', 'geo_point_2d', 'dat' ],
                            convert : {
                                'intitule' : 'label',
                                'type' : '_type'
                            },
                            dataTypes : {
                                'date' : 'dateDDMMYYYY',
                                'dat' : 'date'
                            }
                        })),
                        geometry : this._toGeometry(obj.geo_shape)
                    }
                }
            });
}

/* ------------------------------------------------------------ */

// // 11 - Gare SNCF transilien
function D11_GaresSNCF() {
    return Utils
            .newDataSet({
                "path" : "sncf-gares-et-arrets-transilien-ile-de-france.csv",
                "url" : "http://ressources.data.sncf.com/explore/dataset/sncf-gares-et-arrets-transilien-ile-de-france/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'GaresSNCF'
                        }, this._toProperties(obj, {
                            exclude : [ 'coord_gps_wgs84',
                                    'y_lambert_ii_etendu',
                                    'x_lambert_ii_etendu' ],
                            convert : {
                                'libelle_point_d_arret' : 'label',
                                'type' : '_type'
                            },
                            dataTypes : {
                                'zone_navigo' : 'integer',
                                'gare_non_sncf' : 'boolean'
                            }
                        })),
                        geometry : this._toGeometryPoint(obj.coord_gps_wgs84)
                    }
                }
            });
}

/* ------------------------------------------------------------ */

// 12 - Sanisettes
function D12_Sanisettes() {
    return Utils
            .newDataSet({
                "path" : "sanisettesparis2011.csv",
                "url" : "http://parisdata.opendatasoft.com/explore/dataset/sanisettesparis2011/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'Sanisette'
                        }, this._toProperties(obj, {
                            exclude : [ 'geom', 'geom_x_y' ],
                            convert : {
                                'libelle' : 'label'
                            },
                            dataTypes : {}
                        })),
                        geometry : this._toGeometry(obj.geom)
                    }
                }
            });
}

/* ------------------------------------------------------------ */

// 13 - Kiosques à journaux
function D13_Kiosques() {
    return Utils
            .newDataSet({
                "path" : "carte-des-kiosques-presse-a-paris.csv",
                "url" : "http://data.iledefrance.fr/explore/dataset/carte-des-kiosques-presse-a-paris/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'Kiosque'
                        }, this._toProperties(obj, {
                            exclude : [ 'lat', 'lng' ],
                            convert : {
                                'libelle' : 'label'
                            },
                            dataTypes : {}
                        })),
                        geometry : this._toGeometryPointFromCoords(obj, 'lat',
                                'lng')
                    }
                }
            });
}

/* ------------------------------------------------------------ */

//
// 15 - Marchés de quartier
// !!! No geographic coordinates
function D15_Marches() {
    return Utils
            .newDataSet({
                "path" : "marches-a-paris.csv",
                "url" : "http://opendata.paris.fr/opendata/document?id=134&id_attribute=64",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'Marche'
                        }, this._toProperties(obj, {
                            convert : {},
                            dataTypes : {}
                        })),
                        geometry : this._toGeometryPointFromCoords(obj, 'lat',
                                'lng')
                    }
                }
            });
}
/* ------------------------------------------------------------ */

//
// 16 - Espaces verts, crèches, piscines, équipements sportifs
function D16_EspacesVerts() {
    return Utils
            .newDataSet({
                "path" : "paris_-_liste_des_equipements_de_proximite_ecoles_piscines_jardins.csv",
                "url" : "http://data.iledefrance.fr/explore/dataset/paris_-_liste_des_equipements_de_proximite_ecoles_piscines_jardins/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'EspacePublique'
                        }, this._toProperties(obj, {
                            exclude : [ 'wgs84' ],
                            convert : {
                                'designation_longue' : 'label'
                            },
                            dataTypes : {}
                        })),
                        geometry : this._toGeometryPoint(obj.wgs84)
                    }
                }
            });
}
/* ------------------------------------------------------------ */

// 17 - Liste des sites des hotspots Paris WiFi
function D17_SiteWifi() {
    return Utils
            .newDataSet({
                "path" : "liste_des_sites_des_hotspots_paris_wifi.csv",
                "url" : "http://public.opendatasoft.com/explore/dataset/liste_des_sites_des_hotspots_paris_wifi/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'SpotWifi'
                        }, this._toProperties(obj, {
                            exclude : [ 'geo_coordinates' ],
                            convert : {
                                'nom_site' : 'label'
                            },
                            dataTypes : {}
                        })),
                        geometry : this._toGeometryPoint(obj.geo_coordinates)
                    }
                }
            });
}
/* ------------------------------------------------------------ */

function D24_MobilierParisConfig() {
    return Utils
            .newDataSet({
                "path" : "mobilierenvironnementparis2011.csv",
                "url" : "http://parisdata.opendatasoft.com/explore/dataset/mobilierenvironnementparis2011/download?format=csv",
                transform : function(obj) {
                    return {
                        type : 'Feature',
                        properties : _.extend({
                            type : 'Mobilier'
                        }, this._toProperties(obj, {
                            exclude : [ 'geom', 'geom_x_y' ],
                            convert : {
                                'libelle' : 'label'
                            }
                        })),
                        geometry : this._toGeometry(obj.geom)
                    }
                }
            });
}

/* ------------------------------------------------------------ */

// 25 - La carte des hôtels classés en Île-de-France
function D25_Hotels() {
    var DataTypes = {
        exclude : [ 'lat', 'lng', 'wgs84' ],
        convert : {
            'nom_commercial' : 'label',
            'courriel' : 'email'
        },
        dataTypes : {
            'nombre_de_chambres' : 'integer',
            'capacite_d_accueil_personnes' : 'integer',
            'site_internet' : 'url',
            'telephone' : 'telephone',
            'date_de_classement' : 'date',
            'date_de_publication_de_l_etablissement' : 'date',
            'courriel' : 'email'
        }
    };
    return Utils
            .newDataSet({
                "path" : "les_hotels_classes_en_ile-de-france.csv",
                "url" : "http://data.iledefrance.fr/explore/dataset/les_hotels_classes_en_ile-de-france/download?format=csv",
                transform : function(obj) {
                    var that = this;
                    return Q().then(function() {
                        return {
                            type : 'Feature',
                            properties : _.extend({
                                type : 'Hotels'
                            }, that._toProperties(obj, DataTypes)),
                            geometry : that._toGeometryPoint(obj.wgs84)
                        };
                    })
                }
            });
}

/* ------------------------------------------------------------ */

=======
>>>>>>> 8caf47ce3b85d7b82b1cb661f181347814975c8a
