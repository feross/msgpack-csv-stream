// Dependencies
var cluster = require('cluster')
var csv = require('csv')
var http = require('http')
var knox = require('knox')
var MStream = require('./lib/mstream')
var secret = require('./secret.js')
var url = require('url')
var zlib = require('zlib')

// Constants
var PORT = 3000
var NUM_CPUS = require('os').cpus().length

if (cluster.isMaster) {
  // Spawn one child process per CPU core. They all listen on the same server
  // port and incoming requests are round-robbined between the children. This
  // ensures maxiumum throughput when tasks are CPU-bound.
  for (var i = 0; i < NUM_CPUS; i++) {
    cluster.fork()
  }
  console.log('Listening on port ' + PORT)

} else {
  var s3Client = knox.createClient(secret.s3)
  var server = http.createServer()

  server.on('request', function (req, res) {
    var query = url.parse(req.url, true).query

    if (!query.file)
      return error(res, 400, new Error('Missing required `file` param'))

    s3Client.getFile('/' + query.file, function (err, s3) {
      if (err)
        return error(res, 404, new Error('Not Found'))

      s3.on('error', function (err) {
        error(res, 500, err)
      })

      // Force browser to download file
      res.setHeader('Content-disposition', 'attachment; filename=job.csv')
      res.setHeader('Content-type', 'text/csv')

      // This is the magic!
      s3                            // http response stream from S3
        .pipe(zlib.createGunzip())  // gunzip
        .pipe(MStream())            // binary msgpack -> json
        .pipe(csv())                // json -> lines of csv
        .pipe(res)                  // http response stream to user

    })
  })

  server.listen(PORT)
}

function error (res, code, err) {
  // TODO: log error in database for later examination
  console.log(err.stack)

  // Send error page
  res.statusCode = code
  res.end(code + ' ' + err.message || err)
}