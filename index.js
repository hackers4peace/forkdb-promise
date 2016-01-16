var sprom = require('sprom')
var Promise = require('bluebird')

/**
 * Gets the most recent data for a key.
 *
 * @param {ForkDB} db A ForkDB connection
 * @param {String} key The uri that is used as key
 * @returns {Promise} A promise which evaluates to the document as string
 */
module.exports.get = function (db, key) {
  return new Promise(function (resolve, reject) {
    if (db === 'undefined') {
      reject('A fork db handle must be supplied!')
    }
    if (key === 'undefined') {
      reject('A key must be supplied!')
    }
    sprom.arr(db.forks(key)).then(function (headsArr) {
      if (headsArr.length === 0) {
        return reject('No data for the given key!')
      }
      var readStream = db.createReadStream(headsArr[0].hash)
      sprom.buf(readStream).then(function (data) {
        resolve(data.toString())
      })
    })
  })
}

/**
 * Appends new data to a key. If previous data is found, it is linked
 * correspondingly.
 *
 * @param {ForkDB} db A ForkDB connection
 * @param {String} key The uri that should be used as key
 * @param {String} doc The document that should be stored
 * @returns {Promise} A promise which evaluates to the insert hash.
 */
module.exports.put = function (db, key, doc) {
  return new Promise(function (resolve, reject) {
    if (db === 'undefined') {
      reject('A fork db handle must be supplied!')
    }
    if (key === 'undefined') {
      reject('A key must be supplied!')
    }
    if (doc === 'undefined') {
      reject('A document must be supplied!')
    }
    var meta = { key: key }
    sprom.arr(db.forks(key)).then(function (headsArr) {
      if (headsArr.length > 0) {
        meta.prev = headsArr[0]
      }
      var writeStream = db.createWriteStream(meta, function (err, hash) {
        if (err) reject(err)
        else resolve(hash)
      })
      writeStream.end(doc)
    })
  })
}
