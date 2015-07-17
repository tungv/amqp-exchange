Exchange = require '../index'
_ = require 'lodash'

chai = require('chai')
should = chai.should()
expect = chai.expect


describe 'Connection', ->
  randomQueue = "test queue #{ String(Math.random()).substring 1 }"
  randomExchange = "test.durable.exchange" ##{ String(Math.random()).substring 1 }"

  ex2 = null
  exDurable = null

  ## make sure test queue is bound to test.exchange2 before running any tests
  before (done)->
    ex2 = new Exchange 'test.exchange2', {host:'localhost'}, {logLevel:'FATAL'}

    ex2.exchangeReady
      .then -> ex2.getQueue(randomQueue)
      .then -> ex2.bindQueue(randomQueue, 'long.#.pattern')
      .then -> done()

  after ->
    ex2.queues[randomQueue].destroy()

  before (done)->
    exDurable = new Exchange randomExchange, {host: 'localhost'}, {durable: true, logLevel: 'FATAL'}
    exDurable.exchangeReady.then -> done()

  after ->
    exDurable.exchange.destroy()

  it 'should connect', (done)->
    ex = new Exchange 'test.exchange', {host:'localhost'}, {logLevel:'WARN'}
    ex.exchangeReady.then (ex) -> done()

  it 'should be able to subscribe and push immediately', (done)->
    ex = new Exchange 'test.exchange2', {host:'localhost'}, {logLevel:'WARN'}
    ex.subscribe randomQueue, (message, headers, deliveryInfo, messageObj) ->
      message.text.should.equal 'test message'
      done()

    ex.push {text: 'test message'}, {}, 'long.rabbitmq.pattern'

  it 'should throw error when exchange precondition is not met', ->
    ex = new Exchange randomExchange, {host: 'localhost'}, {durable: false, logLevel: 'FATAL'}
    ex.exchangeReady.catch (err)->
      console.log 'message', err.message
      err.message.should.match /PRECONDITION_FAILED/

describe 'Acknowledgement', ->
  ex = null
  before (done)->
    ex = new Exchange 'test.exchange.three', {host:'localhost'}, {logLevel:'FATAL'}

    ex.exchangeReady
      .then -> ex.getQueue('ack-queue-test')
      .then -> ex.bindQueue('ack-queue-test', 'ack.ack')
      .then -> done()

  it.skip "should queue message", (done)->
    results = []

    ex.subscribe 'ack-queue-test', (message, headers, deliveryInfo, messageObj) ->
      number = message.number
      setTimeout ->
        results.push number
        console.log 'results', results, results.length
        if results.length is 3
          results[0].should.equal 30
          # expect(results).to.eql [30,15,1]
          done()

      , number

    ex.push {number: 30}, {}, 'ack.ack'
    ex.push {number: 15}, {}, 'ack.ack'
    ex.push {number: 1}, {}, 'ack.ack'
