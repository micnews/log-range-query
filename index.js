
var fs     = require('fs')
var glob   = require('pull-glob')
var pull   = require('pull-stream')
var pair   = require('pull-pairs')
var path   = require('path')
var toPull = require('stream-to-pull-stream')
var split  = require('pull-split')
var CSV    = require('csv-line')

function rangeFiles (dir, opts) {

  var gt = opts.gt ? new Date(opts.gt) : null
  var lt = opts.lt ? new Date(opts.lt) : null

  //get the years

  function pad (s) {
    return s < 10 ? '0'+s : ''+s
  }

  return pull(
    glob(path.join(dir, '*/*/*/*')),
    pull.map(function (s) {
      return new Date(path.basename(s))
    }),
    pair(function (a, b) {
      //we want to filter out the files which may contain
      //records in within the range.
      //since the file name is the START of the range
      //if (gt < b && gt > a) return a
      //or (lt < a) return a

      if(a && (
        gt ? gt < b : true
      ) && (
        lt ? lt > a : true
      ))
        return a
    }),
    pull.map(function (date) {
      return path.join(
        dir,
        ''+ date.getFullYear(),
        pad(date.getMonth()),
        pad(date.getDay()),
        date.toISOString()
      )
    })

  )
}

module.exports = queryPullStream

function queryPullStream (opts) {

  var gt = new Date(opts.gt || 0)
  var lt = new Date(opts.lt || Date.now())

  if(gt == 'Invalid Date')
    throw new Error('gt is InvalidDate')
  if(lt == 'Invalid Date')
    throw new Error('lt is InvalidDate')

  var dir = opts.dir

  //by default, assume the log is csv, and that the first
  //field is the timestamp.
  //it's much much faster to use this regexp than to parse the whole line.
  //we are assuming that the file was validated before it was written out.

  var rx = /^(\d+),/

  var filter = opts.filter || function (l) {
    var m = rx.exec(l)
    if(m) {
      var ts = +m[1]
      return (gt ? gt < ts : true) && (lt ? ts < lt : true)
    }
  }

  return pull(
    rangeFiles(dir, {gt: gt, lt: lt}),
    pull.map(function (f) {
      return toPull.source(fs.createReadStream(f))
    }),
    pull.flatten(),
    // keep the newline at the end so that we can just
    // write it out if we want.
    split(null, function (e) { return e + '\n' }),
    pull.filter(filter)
  )

}

if(!module.parent) {

  var opts = require('minimist')(process.argv.slice(2))
  opts.dir = opts.dir || opts._[0]

  var b = ''
  pull(queryPullStream(opts), pull.drain(function (d) {
    b += d + '\n'
    if(b.length > 30000) {
      process.stdout.write(b)
      b = ''
    }
  }, function (e) {
    process.stdout.write(b)
  }))
}

