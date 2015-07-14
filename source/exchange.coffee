_ = require 'lodash'
Table = require 'cli-table'
{EventEmitter} = require 'events'
rabbit = require 'amqp'
log4js = require 'log4js'
domain = require 'domain'

json = (obj) -> JSON.stringify obj, null, ''

logTable = (logger, table, level='trace', delay=false)->
  fn = ->
    return if table._logged and delay
    table._logged = true
    logger[level] '\n%s', table.toString()

  if delay
    setTimeout fn, delay
  else
    fn()

module.exports = class Exchange

  constructor: (@exchangeName, amqpConfig, opts={}) ->
    logger = @logger = log4js.getLogger @exchangeName
    logLevel = opts.logLevel or 'trace'

    @logger.setLevel logLevel


    @exchangeName.should.be.a 'string', 'argument exchangeName is required'
    @exchangeName.length.should.not.equal 0, 'argument exchangeName may not be empty'
    logger.debug 'connecting to amqp', amqpConfig

    connection = @connection = rabbit.createConnection amqpConfig

    @_pushQueue = []
    @_subQueue = []


    _info = @info = new Table {
      head: ['', 'RabbitMQ Connection Info']
      style: compact: true
    }

    _info.push(
      ["connection", "connecting to #{ json(amqpConfig).bold }".yellow]
      ["exchange", "pending".grey]
      ["queue", "pending".grey]
      ["push", "not ready".red]
      ["subscribe", "not ready".red]
    )

    _timestamps = @timestamps =
      init: Date.now()
      connection: Date.now()


    ## cache
    @queues = {}

    ## default push (before exchange being connected)
    ## only push message to queue and wait for exchange to be connected
    @push = ->
      logger.debug 'calling push (queued)'
      @_pushQueue.push arguments

    @subscribe = ->
      logger.debug 'calling subscribe (queued)'
      @_subQueue.push arguments


    ## START CONNECTING
    ## ----------------

    logger.debug 'connecting to exchange', @exchangeName
    logTable logger, _info, 'fatal', 2000

    @exchangeReady = new Promise (resolve, reject)=>

      d = domain.create()

      d.on 'error', (err)->
        logger.warn 'uncaught exception for this connection', err
        reject err

      d.add connection

      d.run =>
        connection.once 'ready', =>
          now = Date.now()
          duration = now - _timestamps.connection
          _timestamps.connection = now

          logger.debug 'CONNECTION ..... OK\t'.bold.green
          logger.trace 'connection ready. Connecting to exchange...'

          _info[0] = [
            "connection"
            "connected to #{ json(amqpConfig).bold } in #{ String(duration).bold }ms"
          ]

          _info[1] = [
            "exchange"
            "connecting to #{ @exchangeName.bold }".yellow
          ]

          _timestamps.exchange = Date.now()
          logTable logger, _info, 'fatal', 200

          try
            @exchange = connection.exchange @exchangeName, opts, (err) =>
              now = Date.now()
              duration = now - _timestamps.exchange
              _timestamps.exchange = now

              _info[1] = [
                "exchange"
                "connected to #{ @exchangeName.bold } in #{ String(duration).bold}ms"
              ]

              try
                logger.debug 'EXCHANGE ....... OK\t'.bold.green
                logger.trace 'exchange [%s] connected!', @exchangeName

                delete @push
                delete @subscribe

                duration = String now - _timestamps.init
                _info[3] = ['push', "ready in #{ duration.bold }ms, queued: #{@_pushQueue.length}"]
                _info[4] = ['subscribe', "ready in #{ duration.bold }ms, queued: #{@_subQueue.length}"]


                ## push queued messages
                @subscribe params... for params in @_subQueue
                @push params... for params in @_pushQueue

                delete @_pushQueue
                delete @_subQueue


                resolve @exchange
                logTable logger, _info, 'info', 100

              catch ex
                logger.warn 'error exchange', ex
                reject ex
          catch ex

            logger.warn 'error exchange', ex
            reject ex

  getQueue: (queueName) ->
    {logger, info: _info, timestamps: _timestamps} = @

    logger.debug "getting queue #{ queueName.bold }"

    opts = {durable: true, autoDelete: false}

    if @queues[queueName]
      logger.debug 'got queue %s from cached', queueName.bold
      return Promise.resolve @queues[queueName]

    return new Promise (resolve, reject) =>
      logger.debug 'inside promise'

      _info[2] = [
        'queue'
        "connecting to queue #{ queueName } with options #{ json(opts).bold }".yellow
      ]


      _timestamps.queue = Date.now()
      queue = @connection.queue queueName, opts, =>
        now = Date.now()
        duration = now - _timestamps.queue
        _timestamps.queue = now
        _info[2] = [
          'queue'
          "connected to queue #{ queueName.bold } with options #{ json(opts).bold } in #{ String(duration).bold}ms"
        ]

        try
          logger.debug 'QUEUE .......... OK'.bold.green
          logger.trace 'queue %s connected!', queueName

          logTable logger, _info, 'info'

          @queues[queueName] = queue
          resolve queue

        catch ex
          logger.warn 'queue error', ex
          reject ex

  push: (message, headers, key)->
    @logger.trace 'calling push (immediate)', key
    @exchange.publish key, message, {
      headers
      deliveryMode: 2
    }

  subscribe: (queueName, handler, opts={ack: false})->
    logger = @logger
    @logger.trace 'calling subscribe (immediate)'

    _timestamps = @timestamps
    _info = @info

    @getQueue(queueName).then (queue)->
      logger.trace "queue.subscribe {ack: false}, (message, headers, deliveryInfo, messageObj)"
      queue.subscribe opts, (message, headers, deliveryInfo, messageObj) ->
        logger.debug 'received message', message.event
        logger.debug ' > event=%s', message.event
        logger.trace ' > ', message, 'retry=', deliveryInfo.redelivered
        handler arguments...


  bindQueue: (queueName, pattern)->
    logger = @logger

    logger.trace 'bindQueue queueName=%s pattern=%s', queueName.bold, pattern.bold
    Promise
      .all([@exchangeReady, @getQueue queueName])
      .then ([ex, queue]) ->
        logger.debug 'ex and queue connected'
        queue.bind ex, pattern, ->
          logger.debug 'queue [%s] bound to pattern %s', queueName, pattern

  unbindQueue: (queueName, pattern)->
    logger = @logger

    logger.trace 'bindQueue queueName=%s pattern=%s', queueName.bold, pattern.bold
    Promise
      .all([@exchangeReady, @getQueue queueName])
      .then ([ex, queue]) ->
        logger.debug 'ex and queue connected'
        queue.unbind ex, pattern, ->
          logger.debug 'queue [%s] bound to pattern %s', queueName, pattern
