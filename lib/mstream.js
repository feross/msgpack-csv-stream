var msgpack = require('msgpack')
var stream = require('stream')
var util = require('util')

module.exports = MStream

util.inherits(MStream, stream.Transform)

/**
 * Streaming msgpack parser, implemented as a node.js Transform stream.
 *
 * @param {Object} options   Transform stream options. Optional.
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