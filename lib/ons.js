'use strict';

var querystring = require('querystring');
var request = require('request');
var lowdb = require('lowdb');
var _db = require('underscore-db');
var moment = require('moment');
var storage = require('lowdb/file-async');
var async = require('async');
var jsonStat = require('jsonstat');

var BASE_URL = 'http://data.ons.gov.uk/ons/api/data/';

var ONS = function ONS(_ref) {
  var _ref$dbLocation = _ref.dbLocation;
  var dbLocation = _ref$dbLocation === undefined ? 'db.json' : _ref$dbLocation;
  var apiKey = _ref.apiKey;


  var db = lowdb('db.json', { storage: storage }, false);
  db._.mixin(_db);

  return {
    getDatasets: function getDatasets(_ref2, cb) {
      var from = _ref2.from;
      var save = _ref2.save;

      var params = {};
      if (from) params.from = from;
      makeApiCall('datasets.json', params, function (err, res, body) {
        if (err) return cb(err);
        if (res.statusCode === 404) return cb(null, { datasetsCount: 0 });
        if (res.statusCode !== 200) return cb(res.statusMessage);
        var data = JSON.parse(body);
        var contexts = data.ons.datasetList.contexts.context;
        if (save) return saveDatasets(contexts, cb);
        cb(null, contexts);
      });
    },
    getGeography: function getGeography(_ref3, cb) {
      var levels = _ref3.levels;
      var hierarchy = _ref3.hierarchy;
      var save = _ref3.save;

      hierarchy = hierarchy || '2011WARDH';
      var params = {};
      params.levels = levels || '1';
      if (params.levels instanceof Array) params.levels = params.levels.join(',');
      makeApiCall('hierarchies/hierarchy/' + hierarchy + '.json', params, function (err, res, body) {
        if (err) return cb(err);
        var data = JSON.parse(body);
        if (save) return saveGeography(data.ons.geographyList, cb);
        cb(null, data.ons.geographyList);
      });
    },
    updateDatasets: function updateDatasets(cb) {
      var lastUpdate = db('meta').getById('UPDATED_AT');
      var params = lastUpdate ? {
        from: moment(lastUpdate.dateTime).format('DD-MM-YYYY')
      } : {};
      this.getDatasets(params, cb);
    },
    getConcepts: function getConcepts(_ref4, cb) {
      var save = _ref4.save;

      makeApiCall('concepts.json', { context: 'Census' }, function (err, res, body) {
        if (err) return cb(err);
        if (res.statusCode === 404) return cb(null, { conceptsCount: 0 });
        if (res.statusCode !== 200) return cb(res.statusMessage);
        var data = JSON.parse(body);
        var contexts = data.ons.datasetList.contexts.context;
        if (save) return saveDatasets(contexts, cb);
        cb(null, contexts);
      });
    },
    findArea: function findArea(name) {
      return db('areas').find({ labels: { label: [{ '$': name }] } });
    },
    getParent: function getParent(area) {
      return db('areas').find({ itemCode: area.parentCode });
    },
    getChildren: function getChildren(area) {
      return db('areas').find({ parendCode: area.itemCode });
    },


    db: db
  };

  function saveGeography(geographyList, cb) {
    var geographyId = geographyList.geography.id;
    geographyList.items.item.forEach(function (area) {
      area.geographyId = geographyId;
      area.id = area.itemCode;
      db('areas').insert(area);
    });
    db('meta').insert({ id: 'UPDATED_AT', dateTime: new Date() });
    db.write().then(function (res) {
      return cb(null, { res: res, count: geographyList.items.item.length });
    }).catch(function (err) {
      return cb(err);
    });
  }

  function saveDatasets(contexts, cb) {
    var datasetsCount = contexts.reduce(function (count, context) {
      context.datasets.dataset.forEach(function (dataset) {
        dataset.contextName = context.contextName;
        dataset._id = dataset.id;
        if (dataset.differentiator) dataset.id = dataset.id + '_' + dataset.differentiator;
        db('datasets').insert(dataset);
      });
      return count + context.datasets.dataset.length;
    }, 0);
    db('meta').insert({ id: 'UPDATED_AT', dateTime: new Date() });
    db.write().then(function (res) {
      return cb(null, { res: res, datasetsCount: datasetsCount });
    }).catch(function (err) {
      return cb(err);
    });
  }

  function makeQueryUrl(endpoint, params) {
    params = Object.assign({}, params, {
      apikey: apiKey,
      jsontype: 'json-stat'
    });
    return '' + BASE_URL + endpoint + '?' + querystring.stringify(params);
  }

  function makeApiCall(endpoint, params, cb) {
    request(makeQueryUrl(endpoint, params), function (err, res, body) {
      if (err) return cb(err);
      console.log('Status: ' + res.statusCode);
      cb(null, res, body);
    });
  }
};

module.exports = ONS;