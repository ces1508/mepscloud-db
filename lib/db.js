'use strict'

const defualts = {
  host: '104.131.43.30',
  // host: 'localhost',
  port: 28015,
  // user: 'm3pscl0ud',
  db: 'mepscloud',
  // password: process.env.MEPSCLOUD_SECRET_DATABASE || 'M3psCl0ud...$2017.'
}
const r = require('rethinkdbdash')(defualts)
const co = require('co')
const Promise = require('bluebird')
const utils = require('./utils')
const sleep = require('then-sleep')

class Db {
  constructor (options) {
    options = options || {}
    this.host = options.host || defualts.host
    this.port = options.port || defualts.port
    this.user = options.user || null
    this.password = options.password || null
    this.db = options.db || defualts.db
    this.setup = options.setup || false
  }
  connect (callback) {
    let dataConnection = {
      host: this.host,
      port: this.port,
    }
    if (this.user) {
      dataConnection.user = this.user
      dataConnection.password = this.password
    }
    this.connected = true
    let db = this.db
    let config = this.setup
    let setup = co.wrap(function * () {
      if (config) {
        console.log('configurando por favor espere ...')
        let dbList = yield r.dbList()
        if (dbList.indexOf(db) === -1) {
          yield r.dbCreate(db)
        }

        let dbTables = yield r.db(db).tableList()

        if (dbTables.indexOf('databases') === -1) {
          yield r.db(db).tableCreate('databases')
          yield r.db(db).table('databases').indexCreate('userId')
          yield r.db(db).table('databases').indexWait()
        }

        if (dbTables.indexOf('contacts') === -1) {
          yield r.db(db).tableCreate('contacts')
          yield r.db(db).table('contacts').indexCreate('fisrtName')
          yield r.db(db).table('contacts').indexCreate('lastName')
          yield r.db(db).table('contacts').indexCreate('databaseId')
          yield r.db(db).table('contacts').indexWait()
        }

        if (dbTables.indexOf('users') === -1) {
          yield r.db(db).tableCreate('users')
          yield r.db(db).table('users').indexCreate('plansId')
          yield r.db(db).table('users').indexCreate('email')
          yield r.db(db).table('users').indexCreate('emailPlanId')
          yield r.db(db).table('users').indexCreate('smsPlanId')
          yield r.db(db).table('users').indexWait()
        }

        if (dbTables.indexOf('templateEmails') === -1) {
          yield r.db(db).tableCreate('templateEmails')
          yield r.db(db).table('templateEmails').indexCreate('userId')
          yield r.db(db).table('templateEmails').indexCreate('createdAt')
          yield r.db(db).table('templateEmails').indexWait()
        }
        if (dbTables.indexOf('templateSms') === -1) {
          yield r.db(db).tableCreate('templateSms')
          yield r.db(db).table('templateSms').indexCreate('userId')
          yield r.db(db).table('templateSms').indexWait()
        }

        if (dbTables.indexOf('campaingEmails') === -1) {
          yield r.db(db).tableCreate('campaingEmails')
          yield r.db(db).table('campaingEmails').indexCreate('userId')
          yield r.db(db).table('campaingEmails').indexCreate('createdAt')
          yield r.db(db).table('campaingEmails').indexWait()

        }

        if (dbTables.indexOf('campaingSms') === -1) {
          yield r.db(db).tableCreate('campaingSms')
          yield r.db(db).table('campaingSms').indexCreate('userId')
          yield r.db(db).table('campaingSms').indexCreate('createdAt')
          yield r.db(db).table('campaingSms').indexWait()
        }

        if (dbTables.indexOf('images') === -1) {
          yield r.db(db).tableCreate('images')
          yield r.db(db).table('images').indexCreate('userId')
          yield r.db(db).table('images').indexWait()
        }
        if (dbTables.indexOf('historicSms') === -1) {
          yield r.db(db).tableCreate('historicSms')
          yield r.db(db).table('historicSms').indexCreate('campaingId')
          yield r.db(db).table('historicSms').indexCreate('userId')
          yield r.db(db).table('historicSms').indexCreate('date')
          yield r.db(db).table('historicSms').indexCreate()

        }
        if (dbTables.indexOf('historicEmail') === -1) {
          yield r.db(db).tableCreate('historicEmail')
          yield r.db(db).table('historicEmail').indexCreate('campaingId')
          yield r.db(db).table('historicEmail').indexWait('campaingId')
          yield r.db(db).table('historicEmail').indexCreate('date')
          yield r.db(db).table('historicEmail').indexCreate('userId')
          yield r.db(db).table('historicEmail').indexWait()

        }

        if (dbTables.indexOf('statistics') === -1) {
          yield r.db(db).tableCreate('statistics')
          yield r.db(db).table('statistics').indexCreate('userId')
          yield r.db(db).table('statistics').indexWait('userId')
          yield r.db(db).table('statistics').indexCreate('campaingId')
          yield r.db(db).table('statistics').indexCreate('date')
          yield r.db(db).table('statistics').indexWait()

        }
        if (dbTables.indexOf('plans') === -1) {
          yield r.db(db).tableCreate('plans')
        }
        if (dbTables.indexOf('smsPlans') === -1) {
          yield r.db(db).tableCreate('smsPlans')
        }
        if (dbTables.indexOf('emailPlans') === -1) {
          yield r.db(db).tableCreate('emailPlans')
        }
        if (dbTables.indexOf('payments') === -1) {
          console.log('creating table payments')
          yield r.db(db).tableCreate('payments')
          console.log('configurin index')
          yield r.db(db).table('payments').indexCreate('userId')
          yield r.db(db).table('payments').indexCreate('referenceCode')
          yield r.db(db).table('payments').indexCreate('creditCard')
          yield r.db(db).table('statistics').indexWait()
          console.log('table payments  is ready')
        }
      }
    })

    return Promise.resolve(setup()).asCallback(callback)
  }
  disconnet (callback) {
    if (!this.connected) {
      return Promise.reject(new Error('not connecteded')).asCallback(callback)
    }
    this.connected = false
    return Promise.resolve(this.connection)
      .then((conn) => {
        return conn.close()
      })
  }

  all (collection, key, index,  skip, order, callback) {
    let db = this.db
    if (!skip) skip = 0
    let limit = 100
    let result = null
    let count = null
    if (!order) order = 'createdAt'
    let tasks = co.wrap(function * () {
      if (order === 'firstName') {
        result = yield r.db(db).table(collection).getAll(key, {index: index}).skip(skip).limit(limit).orderBy(r.asc(order))
        count = yield r.db(db).table(collection).getAll(key, {index: index}).count()
      } else {
        result = yield r.db(db).table(collection).getAll(key, {index: index}).skip(skip).limit(limit).orderBy(r.desc(order))
        count = yield r.db(db).table(collection).getAll(key, {index: index}).count()
      }
      let data = {
        data: result,
        count: count
      }
      return Promise.resolve(data)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  allCampaing (collection ,userId, skip, callback) {
    let db = this.db
    let limit = 100
    if (!skip) skip = 0
    let tasks = co.wrap(function * () {
      let result = yield r.db('mepscloud').table(collection).getAll(userId,
        {index: 'userId'}).eqJoin('databaseId',r.db('mepscloud').table('databases')).without([{right:"id"},{right: 'createdAt'}]).zip().orderBy(r.desc('createdAt')).skip(skip).limit(limit)
      let count = yield r.db('mepscloud').table(collection).getAll(userId, {index: 'userId'}).count()
      let data = {
        data: result,
        count: count
      }
      return Promise.resolve(data)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  allTemplateEmails (userId, skip, callback) {
    let db = this.db
    if (!skip) skip = 0
    let limit = 8
    let result = null
    let count = null
    let tasks = co.wrap(function * () {
      let result = yield r.db(db).table('templateEmails').getAll(userId, {index: 'userId'})
        .orderBy(r.desc('createdAt')).skip(skip).limit(limit)
      let count = yield r.db(db).table('templateEmails').getAll(userId, {index: 'userId'}).count()
      let data = {
        data: result,
        count: count
      }
      return Promise.resolve(data)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  createHook (data, callback) {
    let db = this.db

    let task = co.wrap(function * () {
      for (let i = 0; i < data.length; i++) {
        let campaign = yield r.db(db).table('campaingEmails').get(data[i].campaingId)
        if (campaign) {
          let newData = data[i]
          newData.userId = campaign.userId
          let old_data = yield r.db(db).table('historicEmail').getAll(data[i].email, {index: 'email'})
          .filter({campaingId: data[i].campaingId})
          if (newData.event === 'open'  && old_data[0].event === 'click') {
            yield r.db(db).table('historicEmail').getAll(data[i].email, {index: 'email'})
            .filter({campaingId: data[i].campaingId}).update({date: newData.date})
          } else {
             yield r.db(db).table('historicEmail').getAll(data[i].email, {index: 'email'})
            .filter({campaingId: data[i].campaingId}).update({event: newData.event, date: newData.date})
          }
        }
      }
      yield conn.close()
    })
    return Promise.resolve(task())
  }
  create (collection, data, callback) {
    let db = this.db
    let result = null
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      data.createdAt = new Date()
      let query = yield r.db(db).table(collection).insert(data)
      if (query.errors > 0) {
        return Promise.reject(new Error(query.first_error))
      }
      let id = query.generated_keys[0]
      result = yield find(collection, id)
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  createUser (user, callback) {
    user.password = utils.encrypt(user.password)
    let table = 'users'
    let create = this.create.bind(this)
    let findEmail = this.findUserByEmail.bind(this)
    let tasks = co.wrap(function * () {
      let findUser = yield findEmail(user.email)
      if (findUser) {
        return Promise.reject(new Error('user already exists'))
      }
      user.balanceEmail = 0
      user.balanceSms = 0
      let query = yield create(table, user)
      return Promise.resolve(query)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  update (collection, id, data, callback) {
    let db = this.db
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      try {
        data.updatedAt = new Date()
        let result = yield r.db(db).table(collection).get(id).update(data)
        if (result.errors > 0) {
          return Promise.reject(new Error(result.first_error))
        }
        result = yield find(collection, id)
        return Promise.resolve(result)
      } catch (e) {
        return Promise.reject(new Error(` ${collection} not found`))
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  destroy (collection, id, callback) {
    let db = this.db
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      let get = yield find(collection, id)
      let result = yield r.db(db).table(collection).get(id).delete()
      if (result.errros > 0) {
        return Promise.reject(new Error(result.first_error))
      }
      return Promise.resolve(result)
    })

    return Promise.resolve(tasks()).asCallback(callback)
  }

  destroyDatabase (id, callback) {
    let db = this.db
    let tasks = co.wrap(function * () {
      let result = yield r.db(db).table('contacts').filter({database_id: id}).delete()
      if (result.errors > 0) {
        return Promise.reject(new Error(result.first_error))
      }
      let query = yield r.db(db).table('databases').get(id).delete()
      if (query.errors > 0) {
        return Promise.reject(new Error(query.first_error))
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  find (collection, id, callback) {
    let db = this.db
    let tasks = co.wrap(function * () {
      let result = yield r.db(db).table(collection).get(id)
      if (!result) {
        return Promise.reject(new Error('not found'))
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  findUserByEmail (email, callback) {
    let result = null
    let db = this.db
    let tasks = co.wrap(function * () {
      let user = null
      try {
        user = yield r.db(db).table('users').getAll(email, {
          index: 'email'
        })
        user = user[0]
      } catch (e) {
        return Promise.resolve(false)
      }
      return Promise.resolve(user)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  updateNumberContacts (id, callback) {
    let db = this.db
    let tasks = co.wrap(function * () {
      let count = yield r.db(db).table('contacts').getAll(id, {index: 'databaseId'}).count()
      let updated = yield r.db(db).table('databases').get(id).update({contacts: count})
      return Promise.resolve(updated)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  customFind (collection, id, index, row, params, filter, callback) {
    let db = this.db
    let result = []
    filter = filter || {}
    let tasks = co.wrap(function * () {
      let result = yield r.db(db).table(collection).getAll(id, {index: index}).filter(r.row(row).match(`(?i)^${params}`)).filter(filter).limit(100)
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  filterCamapings (collection, row, params, filter, callback){
    let db = this.db
      let result = []
      filter = filter || {}
      let tasks = co.wrap(function * () {
        let result = yield r.db(db).table(collection).filter(r.row(row).match(`(?i)^${params}`)).filter(filter)
        .eqJoin('databaseId',r.db(db).table('databases')).without({right: 'id'},{right: 'createdA'}).zip().orderBy(r.desc('createdAt'))
        .limit(100)
        return Promise.resolve(result)
      })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  findNumberInCamaping (collection, id, index, filter, callback) {
    let db = this.db
    let result = []
    let tasks = co.wrap(function * () {
      let result = yield r.db(db).table(collection).getAll(id, {index: index}).filter(filter)
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  findContactsByNumber (collection ,id, phone, callback) {
    let db = this.db
    let result = []
    let tasks = co.wrap(function * () {
      let number = phone
      let result = yield r.db(db).table('contacts').filter({databaseId: id, phone: number})
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  getReport (id, collection, callback) {
    let db = this.db
    let tasks = co.wrap( function * () {
      try {
          let send = yield r.db(db).table(collection).getAll(id, {index: 'campaingId'}).filter({status: 'send'}).count()
          let failed = yield r.db(db).table(collection).getAll(id, {index: 'campaingId'}).filter({status: 'failed'}).count()
          return Promise.resolve ({
            send: send,
            filed: failed
          })
      } catch (e) {
        return Promise.reject(new Error(e.message))
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  createReport (id, type, callback) {
    let db = this.db
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      let campaing = null
      try {
        if (type === 'sms') {
          campaing = yield find('campaingSms', id)

          delete campaing.id
          delete campaing.userId
          delete campaing.databaseId
        }
        let report = yield r.db(db).table('statistics').getAll(id, {index: 'campaingId'})
        report = report[0]
        delete report.campaingId
        delete report.id
        delete report.userId
        return Promise.resolve({
          campaing: campaing,
          report: report
        })
      } catch (e) {
          return Promise.reject({error: e.message})
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  graphEmailsbyCampaign (id, callback) {
    let db = this.db
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
        let data = yield r.db(db).table('historicEmail').getAll(id, {index: 'campaingId'}).group('event').count()
        return Promise.resolve(data)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  saveStatistics (data, callback) {
    let db = this.db
    let tasks = co.wrap(function * () {
      try {
        let date = Math.round(new Date().getTime()/1000.0)
        data.date = date
        let response = yield r.db(db).table('statistics').insert(data)
        if (response.errors) {
          return Promise.reject(new Error(e.first_error))
        }
        return Promise.resolve(true)
      } catch (e) {
        console.error(e.message)
        return Promise.reject(new Error(e.message))
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  getReportSmsByDay (user, initDay, lastDay ,callback ) {
    let db = this.db
    let tasks = co.wrap(function * () {
      try {
        let send = yield r.db(db).table('statistics').between(initDay, lastDay,{index: 'date'})
          .filter({userId: user,type: 'sms'}).sum('send')
        let failed = yield r.db(db).table('statistics').between(initDay, lastDay,{index: 'date'})
          .filter({userId: user,type: 'sms'}).sum('failed')
        let data = {}
        data.send = send
        data.failed = failed
        data.date = initDay
        return Promise.resolve(data)
      } catch (e) {
        return Promise.reject(new Error(e.message))
      }
    })
    return Promise.resolve(tasks()).asCallback()
  }

  getReportSmsbyDate (user, initDate, lastDate, callback) {
    let db = this.db
    let tasks = co.wrap(function * () {
      try {
        let data = yield r.db(db).table('historicSms').between([user, r.epochTime(initDate)], [user, r.epochTime(lastDate)], {index: 'dateUserId'})
        .map((row) => {
          return {
            event: row('status'),
            date: row('customDate')
          }
        }).group(function (doc)  {
          return doc.pluck('date', 'event')
        }).count()
        return Promise.resolve(data)
      } catch (e) {
        return Promise.reject({error: e.message})
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  getReportEmailbyDate (user, initDate, lastDate ,callback) {
    let db = this.db
    let tasks = co.wrap(function * () {
      try {
       let data = yield r.db(db).table('historicEmail').between([user, initDate], [user, lastDate], {index: 'dateUserId'})
          .map(function (historic) {
            return {
              date: historic('customDate'),
              event: historic('event')
            }
          }).group(function (doc) {
             return doc.pluck('date', 'event')
          }).count()
        return Promise.resolve(data)
      } catch (e) {
        return Promise.reject(new Error(e.message))
      }
    })
    return Promise.resolve(tasks()).asCallback()
  }
  authenticate (email, password, callback) {
    let getUserByEmail = this.findUserByEmail.bind(this)
    let tasks = co.wrap(function * () {
      let user = null
      try {
        user = yield getUserByEmail(email)
        if (user) {
          if (user.password === utils.encrypt(password)) {
            return Promise.resolve(true)
          } else {
            return Promise.resolve(false)
          }
        } else {
          return Promise.resolve(false)
        }
      } catch (e) {
        return Promise.reject(new Error(`error: ${e}`))
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  campaignSended (userId, callback) {
    let db = this.db
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      try {

        let smsSended = yield r.db(db).table('historicSms').getAll(userId, {index: 'userId'}).count()
        let emailSended = yield r.db(db).table('historicEmail').getAll(userId,{index: 'userId'}).count()
        let user = yield find('users', userId)
        let data = {
          sendSms: smsSended,
          sendEmail: emailSended,
          availableSms: user.balanceSms,
          availableEmail : user.balanceEmail
        }
        return Promise.resolve(data)
      } catch (e) {
        return Promise.reject(new Error(`sorry an error ha ocurried : ${e.message}`))
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  countContacts (databaseId, callback) {
    let db = this.db
    let tasks = co.wrap(function * () {
      try {

        let amount = yield r.db(db).table('contacts').getAll(databaseId, {index: 'databaseId'}).count()
        return Promise.resolve(amount)
      } catch (e) {
        return Promise.reject(e.message)
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  activateAccount (token) {
    let db = this.db
    let tasks = co.wrap(function * () {
      let activate = yield r.db(db).table('users').getAll(token, {index: 'ativateToken'}).filter({
        active: false
      })
      if (activate.length > 0) {
        let id = activate[0].id
        yield r.db(db).table('users').get(id).update({ativateToken: null, activate: true})
        return Promise.resolve(true)
      }
      return Promise.resolve(false)
    })
    return Promise.resolve(tasks())
  }
  priceByPlan (type, callback) {
    let db = this.db
    let data = []
    let task = co.wrap(function * () {
      if (type === "SMS" || type === "sms") {
        data = yield r.db(db).table('smsPlans')
        return Promise.resolve(data)
      }
      data = yield r.db(db).table('emailPlans')
      return Promise.resolve(data)
    })
    return Promise.resolve(task()).asCallback(callback)
  }
  createCharge (paymentId, callback) {
    let db = this.db
    let task = co.wrap(function * () {
      let payment = yield r.db(db).table('payments').get(paymentId)
      try {
        if (payment.state === "APPROVED") {
          if (payment.type === "SMS" || payment.type === "sms") {
            yield r.db(db).table('users').get(payment.userId).update({
              balanceSms: r.row('balanceSms').add(payment.amount)
            })
          } else {
            yield r.db(db).table('users').get(payment.userId).update({
              balanceEmail: r.row('balanceEmail').add(payment.amount)
            })
          }
        }
        return Promise.resolve(true)
      } catch (e) {
        return Promise.reject(`error : ${e.message}`)
      }
    })
    return Promise.resolve(task()).asCallback(callback)
  }
  paymentHook (cReference, callback) {
    let db = this.db
    let task = co.wrap(function * () {
      try {
        let payment = yield r.db(db).table('payments').getAll(cReference, {index: 'referenceSale'})
        payment = payment[0]
        if (payment.status === 'PENDING') {
          if (payment.type === "SMS") {
            yield r.db(db).table('payments').get(payment.id)
              .update({
                balanceSms: r.row('balanceSms').add(payment.amount),
                status: "APPROVED",
                updateAt: Math.round(new Date().getTime()/1000.0)
              })
          } else {
            yield r.db(db).table('users').get(payment.userId)
              .update({
                balanceEmail: r.row('balanceEmail').add(payment.amount),
                status: "APPROVED",
                updateAt: Math.round(new Date().getTime()/1000.0)
              })
          }
          return Promise.resolve(true)
        }
        return Promise.resolve(false)
      } catch (e) {
        return Promise.reject(`a error ocurried: ${e.message}`)
      }
    })
    return Promise.resolve(task()).asCallback(callback)
  }
}
module.exports = Db
