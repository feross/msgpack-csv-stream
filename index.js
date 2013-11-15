var cluster = require('cluster')
var csv = require('csv')
var http = require('http')
var knox = require('knox')
var msgpack = require('msgpack')
var secret = require('./secret.js')
var stream = require('stream')
var url = require('url')
var util = require('util')
var zlib = require('zlib')

var PORT = 3000
var NUM_CPUS = require('os').cpus().length

if (cluster.isMaster) {
  // Spawn a one child process per CPU core, to get full CPU utilization
  for (var i = 0; i < NUM_CPUS; i++) {
    cluster.fork()
  }
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

      // This is where the magic happens!
      s3.pipe(zlib.createGunzip())
        .pipe(MStream())
        .pipe(csv())
        .pipe(res)
    })
  })

  server.listen(PORT)
  console.log('Listening on port ' + PORT)
}

function error (res, code, err) {
  // TODO: log error in database for later examination
  console.log(err.stack)

  // Send error page
  res.statusCode = code
  res.end(code + ' ' + err.message || err)
}

util.inherits(MStream, stream.Transform)

/**
 * Streaming msgpack parser
 *
 * @param {Object} options (Optional transform stream options)
 */
function MStream (options) {
  if (!(this instanceof MStream)) return new MStream(options)

  if (!options) options = {}
  options.objectMode = true

  stream.Transform.call(this, options)
  this.buf = null
}

/**
 * Documented here:
 * http://nodejs.org/api/stream.html#stream_transform_transform_chunk_encoding_callback
 *
 * @param  {Buffer|string}   chunk
 * @param  {string}   encoding
 * @param  {function} callback
 */
MStream.prototype._transform = function (chunk, encoding, callback) {
  // Make sure that this.buf reflects the entirety of the unread stream
  // of bytes; it needs to be a single buffer
  if (this.buf) {
    var b = new Buffer(this.buf.length + chunk.length)
    this.buf.copy(b, 0, 0, this.buf.length)
    chunk.copy(b, this.buf.length, 0, chunk.length)

    this.buf = b
  } else {
    this.buf = chunk
  }

  // Consume messages from the stream, one by one
  while (this.buf && this.buf.length > 0) {
    var msg = msgpack.unpack(this.buf)

    // Buffer does not contain complete message
    if (!msg)
      break

    this.push(msg)

    if (msgpack.unpack.bytes_remaining > 0) {
      this.buf = this.buf.slice(
        this.buf.length - msgpack.unpack.bytes_remaining,
        this.buf.length
      )
    } else {
      this.buf = null
    }
  }

  callback(null)
}

/**
 * Documented here:
 * http://nodejs.org/api/stream.html#stream_transform_flush_callback
 *
 * @param  {function} callback
 */
MStream.prototype._flush = function (callback) {
  // When the input stream is finished, the whole msgpack buffer should be
  // consumed (i.e. nothing left). If not, then emit and error.
  if (this.buf) callback(new Error('Unexpected end of MsgPack stream'))
  else callback(null)
}