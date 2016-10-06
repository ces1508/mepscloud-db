'use strict'

const crypto = require('crypto')
const utils = {
  extractTags,
  encrypt
}

function extractTags (text) {
  if (text == null) return []

  let matches = text.match(/#(\w+)/g)

  if (matches === null) return []

  matches = matches.map(normalize)

  return matches
}
function normalize (text) {
  text = text.toLowerCase()

  text = text.replace(/#/g, '')

  return text
}

function encrypt (text) {
  let shasum = crypto.createHash('sha256')
  shasum.update(text)
  return shasum.digest('hex')
}
module.exports = utils
