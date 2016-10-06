'use strict'

const r = require('rethinkdb')
const co = require('co')
const Promise = require('bluebird')
const utils = require('./utils')

const defualts = {
  host: 'localhost',
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
        let dbList = yield r.dbList().run(conn)
        if (dbList.indexOf(db) === -1) {
          yield r.dbCreate(db).run(conn)
        }

        let dbTables = yield r.db(db).tableList().run(conn)

        if (dbTables.indexOf('databases') === -1) {
          yield r.db(db).tableCreate('databases').run(conn)
        }

        if (dbTables.indexOf('contacts') === -1) {
          yield r.db(db).tableCreate('contacts').run(conn)
        }

        if (dbTables.indexOf('users') === -1) {
          yield r.db(db).tableCreate('users').run(conn)
        }

        if (dbTables.indexOf('templateEmails') === -1) {
          yield r.db(db).tableCreate('templateEmails').run(conn)
        }

        if (dbTables.indexOf('campaingemails') === -1) {
          yield r.db(db).tableCreate('campaingemails').run(conn)
        }

        if (dbTables.indexOf('images') === -1) {
          yield r.db(db).tableCreate('images').run(conn)
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

  all (collection, userId, callback) {
    let db = this.db
    let connection = this.connection.bind(this)
    let tasks = co.wrap(function * () {
      let conn = yield connection
      let result = yield r.db(db).table(collection).getAll(userId, {index: 'userId'}).run(conn)
      try {
        result = yield result.toArray()
      } catch (e) {
        return Promise.reject(new Error(`error ${e}`))
      }
      yield conn.close()
      return Promise.resolve(result)
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

    let tasks = co.wrap(function * () {
      let conn = yield connection
      let result = r.db(db).table(collection).get(id).delete().run(conn)
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
    let result = null
    let connection = this.connection
    let db = this.db
    let tasks = co.wrap(function * () {
      let table = 'users'
      let conn = yield connection
      // yield r.db('mepscloud').table(table).indexWait().run(conn)
      let user = r.db(db).table(table).getAll(email, {
        index: 'email'
      }).run(conn)
      try {
        result = yield user
        result = yield result.next()
      } catch (e) {
        return Promise.resolve(false)
      }
      return Promise.resolve(result)
    })
    return Promise.resolve(tasks()).asCallback(callback)
  }
  authenticate (email, password, callback) {
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
}
module.exports = Db
