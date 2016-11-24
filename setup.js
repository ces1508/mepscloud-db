'use strict'
const Db = require('.')

const config = require('./config')

const db = new Db(config.db)

db.connect()
  .then (conn => {
    console.log('database setup')
    process.exit(0)
  })
  .catch (err => {
    console.error(err)
    process.exit(0)
  })
