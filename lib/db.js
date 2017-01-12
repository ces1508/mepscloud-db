'use strict'

const r = require('rethinkdb')
const co = require('co')
const Promise = require('bluebird')
const utils = require('./utils')

const defualts = {
  host: '104.131.43.30',
  port: 28015,
  db: 'mepscloud'
}

class Db {
  constructor (options) {
    options = options || {}
    this.host = options.host || defualts.host
    this.port = options.port || defualts.port
    this.db = options.db || defualts.db
    this.setup = options.setup || false
  }
  connect (callback) {
    this.connection = r.connect({
      host: this.host,
      port: this.port
    })

    let connection = this.connection
    this.connected = true
    let db = this.db
    let config = this.setup
    let setup = co.wrap(function * () {
      let conn = yield connection
      if (config) {
        console.log('configurando por favor espere ...')
        let dbList = yield r.dbList().run(conn)
        if (dbList.indexOf(db) === -1) {
          yield r.dbCreate(db).run(conn)
        }

        let dbTables = yield r.db(db).tableList().run(conn)

        if (dbTables.indexOf('databases') === -1) {
          yield r.db(db).tableCreate('databases').run(conn)
          yield r.db(db).table('databases').indexCreate('userId').run(conn)
          yield r.db(db).table('databases').indexWait().run(conn)
        }

        if (dbTables.indexOf('contacts') === -1) {
          yield r.db(db).tableCreate('contacts').run(conn)
          yield r.db(db).table('contacts').indexCreate('fisrtName').run(conn)
          yield r.db(db).table('contacts').indexCreate('lastName').run(conn)
          yield r.db(db).table('contacts').indexCreate('databaseId').run(conn)
          yield r.db(db).table('contacts').indexWait().run(conn)
        }

        if (dbTables.indexOf('users') === -1) {
          yield r.db(db).tableCreate('users').run(conn)
          yield r.db(db).table('users').indexCreate('plansId').run(conn)
          yield r.db(db).table('users').indexCreate('email').run(conn)
          yield r.db(db).table('users').indexCreate('emailPlanId').run(conn)
          yield r.db(db).table('users').indexCreate('smsPlanId').run(conn)
          yield r.db(db).table('users').indexWait().run(conn)
        }

        if (dbTables.indexOf('templateEmails') === -1) {
          yield r.db(db).tableCreate('templateEmails').run(conn)
          yield r.db(db).table('templateEmails').indexCreate('userId').run(conn)
          yield r.db(db).table('templateEmails').indexCreate('createdAt').run(conn)
          yield r.db(db).table('templateEmails').indexWait().run(conn)
        }
        if (dbTables.indexOf('templateSms') === -1) {
          yield r.db(db).tableCreate('templateSms').run(conn)
          yield r.db(db).table('templateSms').indexCreate('userId').run(conn)
          yield r.db(db).table('templateSms').indexWait().run(conn)
        }

        if (dbTables.indexOf('campaingEmails') === -1) {
          yield r.db(db).tableCreate('campaingEmails').run(conn)
          yield r.db(db).table('campaingEmails').indexCreate('userId').run(conn)
          yield r.db(db).table('campaingEmails').indexCreate('createdAt').run(conn)
          yield r.db(db).table('campaingEmails').indexWait().run(conn)

        }

        if (dbTables.indexOf('campaingSms') === -1) {
          yield r.db(db).tableCreate('campaingSms').run(conn)
          yield r.db(db).table('campaingSms').indexCreate('userId').run(conn)
          yield r.db(db).table('campaingSms').indexCreate('createdAt').run(conn)
          yield r.db(db).table('campaingSms').indexWait().run(conn)
        }

        if (dbTables.indexOf('images') === -1) {
          yield r.db(db).tableCreate('images').run(conn)
          yield r.db(db).table('images').indexCreate('userId').run(conn)
          yield r.db(db).table('images').indexWait().run(conn)
        }
        if (dbTables.indexOf('historicSms') === -1) {
          yield r.db(db).tableCreate('historicSms').run(conn)
          yield r.db(db).table('historicSms').indexCreate('campaingId').run(conn)
          yield r.db(db).table('historicSms').indexCreate('userId').run(conn)
          yield r.db(db).table('historicSms').indexCreate('date').run(conn)
          yield r.db(db).table('historicSms').indexCreate().run(conn)

        }
        if (dbTables.indexOf('historicEmail') === -1) {
          yield r.db(db).tableCreate('historicEmail').run(conn)
          yield r.db(db).table('historicEmail').indexCreate('campaingId').run(conn)
          yield r.db(db).table('historicEmail').indexWait('campaingId').run(conn)
          yield r.db(db).table('historicEmail').indexCreate('date').run(conn)
          yield r.db(db).table('historicEmail').indexCreate('userId').run(conn)
          yield r.db(db).table('historicEmail').indexWait().run(conn)

        }

        if (dbTables.indexOf('statistics') === -1) {
          yield r.db(db).tableCreate('statistics').run(conn)
          yield r.db(db).table('statistics').indexCreate('userId').run(conn)
          yield r.db(db).table('statistics').indexWait('userId').run(conn)
          yield r.db(db).table('statistics').indexCreate('campaingId').run(conn)
          yield r.db(db).table('statistics').indexCreate('date').run(conn)
          yield r.db(db).table('statistics').indexWait().run(conn)

        }
        if (dbTables.indexOf('plans') === -1) {
          yield r.db(db).tableCreate('plans').run(conn)
        }
        if (dbTables.indexOf('smsPlans') === -1) {
          yield r.db(db).tableCreate('smsPlans').run(conn)
        }
        if (dbTables.indexOf('emailPlans') === -1) {
          yield r.db(db).tableCreate('emailPlans').run(conn)
        }
      }
      return conn
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
    let connection = this.connection.bind(this)
    let tasks = co.wrap(function * () {
      let conn = yield connection
      if (order === 'firstName') {
        result = yield r.db(db).table(collection).getAll(key, {index: index}).skip(skip).limit(limit).orderBy(r.asc(order)).run(conn)
        count = yield r.db(db).table(collection).getAll(key, {index: index}).count().run(conn)
      } else {
        result = yield r.db(db).table(collection).getAll(key, {index: index}).skip(skip).limit(limit).orderBy(r.desc(order)).run(conn)
        count = yield r.db(db).table(collection).getAll(key, {index: index}).count().run(conn)
      }
      try {
        result = yield result.toArray()
      } catch (e) {
        return Promise.reject(new Error(`error ${e}`))
      }
      let data = {
        data: result,
        count: count
      }
      yield conn.close()
      return Promise.resolve(data)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  allCampaing (collection ,userId, skip, callback) {
    let db = this.db
    let limit = 100
    if (!skip) skip = 0
    let connection = this.connection.bind(this)
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let result = yield r.db('mepscloud').table(collection).getAll(userId,
        {index: 'userId'}).eqJoin('databaseId',r.db('mepscloud').table('databases')).without([{right:"id"},{right: 'createdAt'}]).zip().orderBy(r.desc('createdAt')).skip(skip).limit(limit).run(conn)

      let count = yield r.db('mepscloud').table(collection).getAll(userId, {index: 'userId'}).count().run(conn)
      try {
        result = yield result.toArray()
      } catch (e) {
        return Promise.reject(new Error(`error ${e}`))
      }
      let data = {
        data: result,
        count: count
      }
      yield conn.close()
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
    let connection = this.connection
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let result = yield r.db(db).table('templateEmails').getAll(userId, {index: 'userId'})
        .orderBy(r.desc('createdAt')).skip(skip).limit(limit).run(conn)
      let count = yield r.db(db).table('templateEmails').getAll(userId, {index: 'userId'}).count().run(conn)
      try {
        result = yield result.toArray()
      } catch (e) {
        return Promise.reject(new Error(`Error: ${e.message}`))
      }
      let data = {
        data: result,
        count: count
      }
      yield conn.close()
      return Promise.resolve(data)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  createHook (data, callback) {
    let db = this.db
    let connection = this.connection
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let campaign = yield find('campaingEmails', data.campaingId)
      if (campaign) {
        data.userId = campaign.userId
        //let findbyemail = yield r.db(db).table('historicEmail').getAll(data.email,{index: 'email'}).filter({campaingId: data.campaingId}).count().run(conn)
        let old_data = yield r.db(db).table('historicEmail').getAll(data.email,{index: 'email'}).filter({campaingId: data.campaingId}).run(conn)
        old_data =  yield old_data.toArray()
        if (old_data.length > 0) {
          if (data.event === 'open') {
            if (old_data[0].event !== 'click') {
              yield r.db(db).table('historicEmail').getAll(data.email,{index: 'email'}).filter({campaingId: data.campaingId})
                .update({event: data.event, date: data.date, userId: data.userId}).run(conn)
                yield conn.close()
                return Promise.resolve(true)
            }
          } else {
            yield r.db(db).table('historicEmail').getAll(data.email,{index: 'email'}).filter({campaingId: data.campaingId})
                .update({date: data.date, userId: data.userId}).run(conn)
                yield conn.close()
          }
        } else {
           // yield r.db(db).table('historicEmail').insert(data).run(conn)
        }
        return Promise.resolve(true)
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  create (collection, data, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no connected to database'))
    }
    let db = this.db
    let connection = this.connection
    let result = null
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      let conn = yield connection
      data.createdAt = new Date()
      let query = yield r.db(db).table(collection).insert(data).run(conn)
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
    user.state = false
    let create = this.create.bind(this)
    let findEmail = this.findUserByEmail.bind(this)
    let tasks = co.wrap(function * () {
      let findUser = yield findEmail(user.email)
      if (findUser) {
        return Promise.reject(new Error('user already exists'))
      }
      user.balanceEmail = 0
      user.balanceSms = 0
      user.smsPlanId = '4f2c7ca4-e431-43b4-a34e-314970e80951'
      user.emailPlanId = 'f74a0744-7b18-4ae6-9a49-0946d3486019'
      let query = yield create(table, user)
      return Promise.resolve(query)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  update (collection, id, data, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no connected to database'))
    }
    let connection = this.connection
    let db = this.db
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      let conn = yield connection
      try {
        data.updatedAt = new Date()
        let result = yield r.db(db).table(collection).get(id).update(data).run(conn)
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
    if (!this.connected) {
      return Promise.reject(new Error('no connected to database'))
    }

    let db = this.db
    let connection = this.connection
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let get = yield find(collection, id)
      let result = yield r.db(db).table(collection).get(id).delete().run(conn)
      if (result.errros > 0) {
        return Promise.reject(new Error(result.first_error))
      }
      return Promise.resolve(result)
    })

    return Promise.resolve(tasks()).asCallback(callback)
  }

  destroyDatabase (id, callback) {
    let connection = this.connection
    let db = this.db
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let result = yield r.db(db).table('contacts').filter({database_id: id}).delete().run(conn)
      if (result.errors > 0) {
        return Promise.reject(new Error(result.first_error))
      }
      let query = yield r.db(db).table('databases').get(id).delete().run(conn)
      if (query.errors > 0) {
        return Promise.reject(new Error(query.first_error))
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  find (collection, id, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
    let connection = this.connection
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let result = yield r.db(db).table(collection).get(id).run(conn)
      if (!result) {
        return Promise.reject(new Error('not found'))
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  findUserByEmail (email, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let result = null
    let connection = this.connection
    let db = this.db
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let user = yield r.db(db).table('users').getAll(email, {
        index: 'email'
      }).run(conn)
      try {
        result = yield user.next()
      } catch (e) {
        return Promise.resolve(false)
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }

  updateNumberContacts (id, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
    let connection = this.connection
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let count = yield r.db(db).table('contacts').getAll(id, {index: 'databaseId'}).count().run(conn)
      let updated = yield r.db(db).table('databases').get(id).update({contacts: count}).run(conn)
      return Promise.resolve(updated)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  customFind (collection, id, index, row, params, filter, callback) {

    let db = this.db
    let connection = this.connection
    let result = []
    filter = filter || {}
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let query = yield r.db(db).table(collection).getAll(id, {index: index}).filter(r.row(row).match(`(?i)^${params}`)).filter(filter).limit(100).run(conn)
      try {
        result = yield query.toArray()
      } catch (e) {
        return Promise.reject(new Error(e.message))
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  filterCamapings (collection, row, params, filter, callback){
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
      let connection = this.connection
      let result = []
      filter = filter || {}
      let tasks = co.wrap(function * () {
        let conn = yield connection
        let query = yield r.db(db).table(collection).filter(r.row(row).match(`(?i)^${params}`)).filter(filter)
        .eqJoin('databaseId',r.db(db).table('databases')).without({right: 'id'},{right: 'createdA'}).zip().orderBy(r.desc('createdAt'))
        .limit(100).run(conn)
        try {
          result = yield query.toArray()
        } catch (e) {
          return Promise.reject(new Error(e.message))
        }
        return Promise.resolve(result)
      })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  findNumberInCamaping (collection, id, index, filter, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
    let connection = this.connection
    let result = []
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let query = yield r.db(db).table(collection).getAll(id, {index: index}).filter(filter).run(conn)
      try {
        result = yield query.toArray()
      } catch (e) {
        return Promise.reject(new Error(e.message))
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  findContactsByNumber (collection ,id, phone, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
    let connection = this.connection
    let result = []
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let number = phone
      let query = yield r.db(db).table('contacts').filter({databaseId: id, phone: number}).run(conn)
      try {
        result = yield query.toArray()
      } catch (e) {
        return Promise.reject(new Error(e.message))
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  getReport (id, collection, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
    let connection = this.connection
    let tasks = co.wrap( function * () {
      try {
        let conn = yield connection
          let send = yield r.db(db).table(collection).getAll(id, {index: 'campaingId'}).filter({status: 'send'}).count().run(conn)
          let failed = yield r.db(db).table(collection).getAll(id, {index: 'campaingId'}).filter({status: 'failed'}).count().run(conn)
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
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
    let connection = this.connection
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      let campaing = null
      try {
        let conn = yield connection
        if (type === 'sms') {
          campaing = yield find('campaingSms', id)

          delete campaing.id
          delete campaing.userId
          delete campaing.databaseId
        }
        let report = yield r.db(db).table('statistics').getAll(id, {index: 'campaingId'}).run(conn)
        report = yield report.toArray()
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
    let connection = this.connection
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
       let data = {
          open: 0,
          click: 0,
          delivered: 0,
          processed: 0,
          dropped: 0,
          spam: 0,
          deferred: 0,
          bounce: 0,
          unsubscribe: 0,
          block: 0,
          sending: 0
        }
        let conn = yield connection
        let campaign = yield find('campaingEmails', id)
        data.open = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'open'}).count().run(conn)
        data.click = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'click'}).count().run(conn)
        data.delivered = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'delivered'}).count().run(conn)
        data.processed = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'processed'}).count().run(conn)
        data.dropped = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'droppped'}).count().run(conn)
        data.spam = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'spam'}).count().run(conn)
        data.deferred = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'deferred'}).count().run(conn)
        data.bounce = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'bounce'}).count().run(conn)
        data.unsubscribe = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'unsubscribe'}).count().run(conn)
        data.block = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'block'}).count().run(conn)
        data.sending = yield r.db(db).table('historicEmail').getAll(id,{index: 'campaingId'}).filter({event: 'sending'}).count().run(conn)
        return Promise.resolve(data)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  saveStatistics (data, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let db = this.db
    let connection = this.connection
    let tasks = co.wrap(function * () {
      try {
        let conn = yield connection
        let date = Math.round(new Date().getTime()/1000.0)
        data.date = date
        let response = yield r.db(db).table('statistics').insert(data).run(conn)
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
  reportSmsWeek (user, date, callback) {

  }
  getReportSmsByDay (user, initDay, lastDay ,callback ) {
    let db = this.db
    let connection = this.connection
    let tasks = co.wrap(function * () {
      try {
        let conn = yield connection
        let send = yield r.db(db).table('statistics').between(initDay, lastDay,{index: 'date'})
          .filter({userId: user,type: 'sms'}).sum('send').run(conn)
        let failed = yield r.db(db).table('statistics').between(initDay, lastDay,{index: 'date'})
          .filter({userId: user,type: 'sms'}).sum('failed').run(conn)
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
  getReportEmailByDay (user, initDay, lastDay ,callback) {
    let db = this.db
    let connection = this.connection
    let tasks = co.wrap(function * () {
      try {
        let conn = yield connection
       let data = {
          open: 0,
          click: 0,
          delivered: 0,
          processed: 0,
          droppped: 0,
          spam: 0,
          deferred: 0,
          bounce: 0,
          unsubscribe: 0,
          block: 0,
          sending: 0,
          date: initDay,
        }
        data.open = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'open'}).count().run(conn)
        data.click = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'click'}).count().run(conn)
        data.delivered = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'delivered'}).count().run(conn)
        data.processed = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'processed'}).count().run(conn)
        data.droppped = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'droppped'}).count().run(conn)
        data.spam = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'spam'}).count().run(conn)
        data.deferred = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'deferred'}).count().run(conn)
        data.bounce = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'bounce'}).count().run(conn)
        data.unsubscribe = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'unsubscribe'}).count().run(conn)
        data.block = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'block'}).count().run(conn)
        data.sending = yield r.db(db).table('historicEmail').between(initDay, lastDay,{index: 'date'}).filter({userId: user, event: 'sending'}).count().run(conn)

        return Promise.resolve(data)
      } catch (e) {
        return Promise.reject(new Error(e.message))
      }
    })
    return Promise.resolve(tasks()).asCallback()
  }
  authenticate (email, password, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }
    let get = this.findUserByEmail.bind(this)
    let tasks = co.wrap(function * () {
      let user = null
      try {
        user = yield get(email)
        if (user.password === utils.encrypt(password)) {
          return Promise.resolve(true)
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
    let connection = this.connection
    let find = this.find.bind(this)
    let tasks = co.wrap(function * () {
      try {
        let conn = yield connection
        console.log(conn)
        let smsSended = yield r.db(db).table('historicSms').getAll(userId, {index: 'userId'}).count().run(conn)
        let emailSended = yield r.db(db).table('historicEmail').getAll(userId,{index: 'userId'}).count().run(conn)
        let user = yield find('users', userId)
        let priceSms = yield find('smsPlans', user.smsPlanId)
        let priceEmails = yield find('emailPlans', user.emailPlanId)
        let data = {
          sms: smsSended,
          email: emailSended,
          priceSms: priceSms.price,
          priceEmail : priceEmails.price
        }
        console.log(data)
        return Promise.resolve(data)
      } catch (e) {
        return Promise.reject(new Error(`sorry an error ha ocurried : ${e.message}`))
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  countContacts (databaseId, callback) {
    if (!this.connected) {
      return Promise.reject(new Error('no conneted to database'))
    }

    let connection = this.connection
    let db = this.db
    let tasks = co.wrap(function * () {
      try {
        let conn = yield connection
        let amount = yield r.db(db).table('contacts').getAll(databaseId, {index: 'databaseId'}).count().run(conn)
        return Promise.resolve(amount)
      } catch (e) {
        return Promise.reject(e.message)
      }
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
}
module.exports = Db
