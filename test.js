var ONS = require('.')
var config = require('./config.json')
var ons = ONS({ apiKey: config.apiKey })

// ons.updateDatasets(function (err, res) { console.log(`Updated ${res.datasetsCount} datasets`)})
// ons.getGeography({ levels: [1, 2, 3, 4, 5, 6, 7], save: true }, function (err, res) { console.log(err, res) })

module.exports = ons
