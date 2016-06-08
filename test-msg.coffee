require 'log-a-log'

_ = require 'lodash'
Q = require 'q'
uuid = require 'uuid'
pubnub = require 'pubnub'
moment = require 'moment'
durations = require 'durations'
WebSocket = require 'ws'

now = -> moment().valueOf()

#ws = new WebSocket('ws://localhost:8888')
ws = new WebSocket('ws://52.39.3.158:8888')

initialDelay = 2000
messageDelay = 10
remaining = 200

counts = {}
messages = {}
sent = 0
received = 0
watch = durations.stopwatch()

# Summarize all of the message deliveries
summarize = ->
  totals = _(messages)
  .map ({id, timing: {total}}) ->
    id: id
    total: total
  .sortBy ({total}) -> total

  longest = totals.last().total
  shortest = totals.first().total
  mean = _.sum(totals.map(({total}) -> total).value()) / totals.size()

  console.log "shortest=#{shortest} longest=#{longest} mean=#{mean} : took #{watch}"

# Message handler
ws.on 'message', (json) ->
  try
    {channel, message} = JSON.parse json
    console.log "[#{channel}]> #{message}"
    {id} = JSON.parse message
    msg = messages[id]
    if not msg.timing?
      msg.timing = {}
    count = counts[id]
    if not count?
      console.log "UNKNOWN ===============================> #{id}"
      counts[id] = 1
    else
      if count > 0
        console.log " DUPLICATE ===========================> #{id}"
      count += 1
      counts[id] = count
    msg.timing.received = now()
    received += 1
    msg.timing.delivery = msg.timing.published - msg.timing.sent
    msg.timing.broker = msg.timing.received - msg.timing.published
    msg.timing.total = msg.timing.received - msg.timing.sent
    console.log "Message '#{id}' (#{received} of #{sent}) received in #{msg.timing.total} ms (Delivery #{msg.timing.delivery}, Server #{msg.timing.broker} ms): #{json}"

    if remaining < 1 and received >= sent
      summarize()
      ws.close()
  catch error
    console.error "Error parsing message: #{error}\n#{error.stack}"

# recursive message delivery function
sendMsg = ->
  if remaining > 0
    remaining -= 1
    Q.delay messageDelay
    .then ->
      msg =
        id: uuid.v1()
        timing:
          created: now()

      console.log "Sending message #{msg.id}..."
      messages[msg.id] = msg
      counts[msg.id] = 0

      json = JSON.stringify msg
      msg.timing.sent = now()

      try
        message =
          action: 'publish'
          channel: 'messages'
          message: json
        ws.send JSON.stringify(message)

        sent += 1
        ts = now()
        msg.timing.published = ts
        pubTime = msg.timing.published - msg.timing.sent
        console.log "Message ##{sent} '#{msg.id}' delivered in #{pubTime} ms"
        sendMsg()
      catch error
        console.error "Error sending message: #{error}\n#{error.stack}"
        msg.error = true
        sendMsg()

# connect after 2 sec. to give the subscription some time to "take"
ws.on 'open', ->
  try
    # Subscribe to the channel
    subscription =
      action: 'subscribe'
      channel: 'messages'
    ws.send JSON.stringify(subscription)
  catch error
    console.error "Error subscribing to channel: #{error}\n#{error.stack}"

  Q.delay initialDelay
  .then ->
    watch.start()
    sendMsg()

ws.on 'close', ->
  console.log "Closed."

