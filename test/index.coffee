Exchange = require '../index'
_ = require 'lodash'

describe 'Connection', ->
  randomQueue = "test queue #{ String(Math.random()).substring 1 }"
  ex2 = null

  ## make sure test queue is bound to test.exchange2 before running any tests
  before (done)->
    ex2 = new Exchange 'test.exchange2', {host:'localhost'}, {logLevel:'FATAL'}

    ex2.exchangeReady
      .then -> ex2.getQueue(randomQueue)
      .then -> ex2.bindQueue(randomQueue, 'long.#.pattern')
      .then -> done()

  after ->
    ex2.queues[randomQueue].destroy()

  it 'should connect', (done)->
    ex = new Exchange 'test.exchange', {host:'localhost'}, {logLevel:'WARN'}
    ex.exchangeReady.then (ex) -> done()

  it 'should be able to subscribe and push immediately', (done)->
    ex = new Exchange 'test.exchange2', {host:'localhost'}, {logLevel:'WARN'}
    ex.subscribe randomQueue, (message, headers, deliveryInfo, messageObj) ->
      message.text.should.equal 'test message'
      done()

    ex.push {text: 'test message'}, {}, 'long.rabbitmq.pattern'
