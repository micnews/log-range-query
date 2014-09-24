
var fs      = require('fs')
var glob    = require('pull-glob')
var pull    = require('pull-stream')
var cat     = require('pull-cat')
var pair    = require('pull-pairs')
var path    = require('path')
var split   = require('pull-split')
var toPull  = require('stream-to-pull-stream')
var isodate = require('regexp-isodate')

var date

var debug = !!process.env.DEBUG

function rangeFiles (dir, opts) {

  var globs =
    'string' === typeof opts.globs ? [opts.globs]
    //default to the globs that will match log-rotation-stream
  : opts.globs || ['*/*/*/*', '*']

  var gt = opts.gt ? new Date(opts.gt) : null
  var lt = opts.lt ? new Date(opts.lt) : null

  //get the years

  function pad (s) {
    return s < 10 ? '0'+s : ''+s
  }

  return pull(
    cat(globs.map(function (pattern) {
      return glob(path.join(dir, pattern))
    })),
    pull.map(function (s) {
      var m = isodate.exec(s)
      return m && {filename: s, ts: new Date(path.basename(m[1]))}
    }),
    pull.filter(Boolean),
    pair(function (a, b) {
      // we want to filter out the files which may contain
      // records in within the range.
      // since the file name is the START of the range
      // if (gt < b.ts && gt > a.ts) return a
      // or (lt < a.ts) return a

      // except if it's the last file,
      // then open this file if a.ts < lt
      // gt doesn't count in this case,
      // because we can't say that it's not in the file.

      if (a && (
        gt ? (!b || gt < b.ts) : true
      ) && (
        lt ? lt > a.ts         : true
      ))
        return a

    }),
    pull.map(function (data) {
      if(debug) console.error(data.filename)
      return data.filename
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
    b += d
    if(b.length > 30000) {
      process.stdout.write(b)
      b = ''
    }
  }, function (e) {
    if(e) throw e
    process.stdout.write(b)
  }))
}

