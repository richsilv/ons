const querystring = require('querystring')
const request = require('request')
const lowdb = require('lowdb')
const _db = require('underscore-db')
const moment = require('moment')
const storage = require('lowdb/file-async')
const async = require('async')
const jsonStat = require('jsonstat')

const BASE_URL = 'http://data.ons.gov.uk/ons/api/data/'

var ONS = function ({ dbLocation='db.json', apiKey }) {

  var db = lowdb('db.json', { storage }, false)
  db._.mixin(_db)

  return {
    getDatasets ({ from, save }, cb) {
      var params = {}
      if (from) params.from = from
      makeApiCall('datasets.json', params, (err, res, body) => {
        if (err) return cb(err)
        if (res.statusCode === 404) return cb(null, { datasetsCount: 0 })
        if (res.statusCode !== 200) return cb(res.statusMessage)
        var data = JSON.parse(body)
        var contexts = data.ons.datasetList.contexts.context
        if (save) return saveDatasets(contexts, cb)
        cb(null, contexts)
      })
    },

    getGeography ({ levels, hierarchy, save }, cb) {
      hierarchy = hierarchy || '2011WARDH'
      var params = {}
      params.levels = levels || '1'
      if (params.levels instanceof Array) params.levels = params.levels.join(',')
      makeApiCall(`hierarchies/hierarchy/${hierarchy}.json`, params, (err, res, body) => {
        if (err) return cb(err)
        var data = JSON.parse(body)
        if (save) return saveGeography(data.ons.geographyList, cb)
        cb(null, data.ons.geographyList)
      })
    },

    updateDatasets (cb) {
      const lastUpdate = db('meta').getById('UPDATED_AT')
      const params = lastUpdate ? {
        from: moment(lastUpdate.dateTime).format('DD-MM-YYYY')
      } : {}
      this.getDatasets(params, cb)
    },

    getConcepts ({ save }, cb) {
      makeApiCall('concepts.json', { context: 'Census' }, (err, res, body) => {
        if (err) return cb(err)
        if (res.statusCode === 404) return cb(null, { conceptsCount: 0 })
        if (res.statusCode !== 200) return cb(res.statusMessage)
        var data = JSON.parse(body)
        var contexts = data.ons.datasetList.contexts.context
        if (save) return saveDatasets(contexts, cb)
        cb(null, contexts)
      })
    },

    findArea (name) {
      return db('areas').find({ labels: { label: [{ '$': name }] } })
    },

    getParent (area) {
      return db('areas').find({ itemCode: area.parentCode })
    },

    getChildren (area) {
      return db('areas').find({ parendCode: area.itemCode })
    },

    db: db
  }

  function saveGeography (geographyList, cb) {
    var geographyId = geographyList.geography.id
    geographyList.items.item.forEach(area => {
      area.geographyId = geographyId
      area.id = area.itemCode
      db('areas').insert(area)
    })
    db('meta').insert({id: 'UPDATED_AT', dateTime: new Date()})
    db.write()
      .then(res => cb(null, { res, count: geographyList.items.item.length }))
      .catch(err => cb(err))
  }

  function saveDatasets(contexts, cb) {
    var datasetsCount = contexts.reduce((count, context) => {
      context.datasets.dataset.forEach(dataset => {
        dataset.contextName = context.contextName
        dataset._id = dataset.id
        if (dataset.differentiator) dataset.id = `${dataset.id}_${dataset.differentiator}`
        db('datasets').insert(dataset)
      })
      return count + context.datasets.dataset.length
    }, 0)
    db('meta').insert({id: 'UPDATED_AT', dateTime: new Date()})
    db.write()
      .then(res => cb(null, { res, datasetsCount }))
      .catch(err => cb(err))
  }

  function makeQueryUrl (endpoint, params) {
    params = Object.assign({}, params, {
      apikey: apiKey,
      jsontype: 'json-stat'
    })
    return `${BASE_URL}${endpoint}?${querystring.stringify(params)}`
  }

  function makeApiCall (endpoint, params, cb) {
    request(makeQueryUrl(endpoint, params), (err, res, body) => {
      if (err) return cb(err)
      console.log(`Status: ${res.statusCode}`)
      cb(null, res, body)
    })
  }
}

module.exports = ONS
