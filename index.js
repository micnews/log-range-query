
var fs     = require('fs')
var glob   = require('pull-glob')
var pull   = require('pull-stream')
var pair   = require('pull-pairs')
var path   = require('path')
var toPull = require('stream-to-pull-stream')
var split  = require('pull-split')

var rangeFiles = module.exports = function (dir, opts) {

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
  //}
}

if(!module.parent) {

  var opts = require('minimist')(process.argv.slice(2))

  var gt = new Date(opts.gt || 0)
  var lt = new Date(opts.lt || Date.now())

  if(gt == 'Invalid Date')
    throw new Error('gt is InvalidDate')
  if(lt == 'Invalid Date')
    throw new Error('lt is InvalidDate')

  var dir = opts._[0]

  pull(
    rangeFiles(dir, {gt: gt, lt: lt}),
    pull.map(function (f) {
      return toPull.source(fs.createReadStream(f))
    }),
    pull.flatten(),
    split(),
    pull.drain(console.log)
  )

}

