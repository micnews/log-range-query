# log-range-query

query log files (created by log-rotation-stream) with date ranges.

``` js
var LogRangeQuery = require('log-range-query')

var lrq = LogRangeQuery(dir, function (line, opts) {
  var parsed = CSV.parse(line)
  //suppose column 0 is timestamp.
  var ts = data[0]
  if(opts.gt < ts && ts < opts.lt)
    return data
})

//greater than 1st Jan 1970, less than now. 
lrq({gt: new Date(0), lt: new Date()})
  .pipe(BIGDATA)

```

## License

MIT
