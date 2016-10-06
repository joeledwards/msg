#!/usr/bin/env coffee

require 'log-a-log'

_ = require 'lodash'
P = require 'bluebird'
uuid = require 'uuid'
pubnub = require 'pubnub'
moment = require 'moment'
cluster = require 'cluster'
durations = require 'durations'
WebSocket = require 'ws'
EventEmitter = require 'events'

# Small Server
#uris = ['ws://52.39.3.158:8888']

# Stand-alone test instance
#uris = ['ws://localhost:8888']

# Elixir Servers
uris = [
  'ws://172.20.0.2:8080/pubsub'
  'ws://172.20.0.3:8080/pubsub'
  'ws://172.20.0.4:8080/pubsub'
  'ws://172.20.0.5:8080/pubsub'
  'ws://172.20.0.6:8080/pubsub'
  'ws://172.20.0.7:8080/pubsub'
  'ws://172.20.0.8:8080/pubsub'
  'ws://172.20.0.9:8080/pubsub'
]

# Node.js Servers
uris = [
  'ws://172.18.0.4:8888'
  'ws://172.18.0.5:8888'
  'ws://172.18.0.6:8888'
  'ws://172.18.0.7:8888'
  'ws://172.18.0.8:8888'
  'ws://172.18.0.9:8888'
  'ws://172.18.0.10:8888'
  'ws://172.18.0.11:8888'
]

uriCount = uris.length

# Publisher Tuning Parameters
initialPublishDelayMillis = 2000 # Delay between all sockets connected and first message published
publishDelayMillis = 0 # Delay between message sends on each channel
messagesPerChannel = 1000 # Number of messages to send to each channel
channelCount = 20 # Number of distinct channels
pubWorkers = 2 # Channels are divied up between pub-workers

# Subscriber Tuning Parameters
subscriberCount = 8 # Each subscriber connects to all channels
subWorkers = 2 # Subscribers are divied up between sub-workers

totalPubMessages = channelCount * messagesPerChannel
totalSubMessages = channelCount * messagesPerChannel * subscriberCount
allStop = false

# Run an individual publisher
runPubWorker = ({id, channelGroup}) ->
  console.log "Starting worker #{id}"
  worker = new EventEmitter()

  webSockets = []
  active = 0
  workerSendCount = 0
  summary =
    id: id
    type: 'publisher'

  watch = durations.stopwatch()
  lastSummaryWatch = durations.stopwatch().start()

  # Summarize all of the message deliveries
  summarize = ->
    console.log "[#{id}] publish worker #{id} ran for #{watch}
      \n  published=#{workerSendCount}"

  timedSummary = ->
    if lastSummaryWatch.duration().millis() >= 1000
      summarize()
      lastSummaryWatch.reset().start()

  finalSummary = -> summarize() if active < 1

  workerMessageCount = 0
  totalWs = channelGroup.length
  pubCounter = 0

  # Set up a WebSocket for each channel
  channelGroup.forEach (channel) ->
    closed = false
    pubCounter += 1
    uriOffset = pubCounter % uriCount
    uri = uris[uriOffset]
    ws = new WebSocket(uri)

    channelSendCount = 0
    didConnect = false
    channelWatch = durations.stopwatch()

    ws.once 'open', ->
      didConnect = true
      active += 1
      channelWatch.start()

      console.log "[#{id}] channel '#{channel}' connected (#{active} of #{totalWs}) : #{uri}"

      if active == totalWs
        process.nextTick -> worker.emit('all-ws-connected')

      sendMessage = () ->
        try
          if channelSendCount < messagesPerChannel
            record =
              action: 'publish'
              channel: channel
              message:
                uuid: uuid.v4()
            message = JSON.stringify record

            ws.send message
            workerSendCount += 1
            channelSendCount += 1

            if workerSendCount >= workerMessageCount
              console.log "[#{id}] All #{workerMessageCount} messages published in #{channelWatch} : #{uri}"

            timedSummary()

            setTimeout sendMessage, publishDelayMillis
          else
            console.log "[#{id}] All #{messagesPerChannel} messages published to channel '#{channel}' in #{channelWatch}"

            ws.close()
        catch error
          console.error "[#{id}] Error publishing message:", error

      worker.once 'start', ->
        console.log "[#{id}] Publishing first message to channel '#{channel}'"
        sendMessage()

    ws.once 'close', ->
      if didConnect
        active -= 1

      console.log "[#{id}] channel '#{channel}' disconnected (#{active} of #{totalWs}) : #{uri}"
      closed = true

      if active < 1
        finalSummary()
        summary.published = workerSendCount
        process.nextTick -> worker.emit('all-ws-disconnected')
        process.nextTick -> worker.emit('worker-summary', summary)

    ws.on 'error', (error) ->
      console.error "[#{id}] channel '#{channel}' - error:", error

    webSockets.push ws

  # Set the termination timeout once we get the 'start' signal
  worker.once 'start', ->
    watch.start()

  workerMessageCount = pubCounter * messagesPerChannel

  worker

# Run an individual subscriber
runSubWorker = ({id, subscriberGroup, channels}) ->
  console.log "Starting worker #{id}"
  worker = new EventEmitter()

  console.log "#{id} subscriberGroup:", subscriberGroup
  webSockets = []
  active = 0
  workerRecvCount = 0
  summary =
    id: id
    type: 'subscriber'
  watch = durations.stopwatch()

  lastSummaryWatch = durations.stopwatch().start()

  # Summarize all of the message deliveries
  summarize = ->
    console.log "[#{id}] subscription worker #{id} ran for #{watch}
      \n  received=#{workerRecvCount}"

  timedSummary = ->
    if lastSummaryWatch.duration().millis() >= 1000
      summarize()
      lastSummaryWatch.reset().start()

  if active < 1
    finalSummary = -> summarize()

  workerMessageCount = 0
  totalWs = channels.length * subscriberGroup.length

  subCounter = 0
  channels.forEach (channel) ->
    subscriberGroup.forEach (i) ->
      subCounter += 1
      uriOffset = subCounter % uriCount
      uri = uris[uriOffset]
      ws = new WebSocket(uri)

      channelRecvCount = 0
      didConnect = false
      channelWatch = durations.stopwatch()

      # The WebSocket is connected
      ws.once 'open', ->
        ws.send JSON.stringify({
          action: 'subscribe'
          channel: channel
        })

      # Received a message from the WebSocket
      ws.on 'message', (message) ->
        record = JSON.parse message

        switch record.action
          when 'subscribe'
            if record.result == 'success'
              didConnect = true
              active += 1
              channelWatch.start()

              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' connected (#{active} of #{totalWs}) : #{uri}"

              if active >= totalWs
                process.nextTick -> worker.emit('all-ws-connected')
            else
              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' failed to subscribe"
              ws.close()

          when 'unsubscribe'
            if record.result != 'success'
              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' failed to unsubscribe"

            ws.close()

          when 'message'
            workerRecvCount += 1
            channelRecvCount += 1

            if workerRecvCount >= workerMessageCount
              console.log "[#{id}] All #{workerMessageCount} messages consumed in #{channelWatch}"

            if channelRecvCount >= messagesPerChannel
              console.log "[#{id}] All #{messagesPerChannel} messages consumed from channel '#{channel}' in #{channelWatch}"

              ws.send JSON.stringify({
                action: 'unsubscribe'
                channel: channel
              })
            else
              timedSummary()

          else
            console.error "Unregonized message from pub/sub server!"

      # The WebSocket has been closed
      ws.once 'close', ->
        if didConnect
          active -= 1

        console.log "[#{id}] Subscriber #{i} to channel '#{channel}' disconnected (#{active} of #{totalWs}) : #{uri}"

        if active < 1
          finalSummary()
          summary.received = workerRecvCount
          process.nextTick -> worker.emit('all-ws-disconnected')
          process.nextTick -> worker.emit('worker-summary', summary)

      ws.on 'error', (error) ->
        console.error "[#{id}] Subscriber #{i} to channel '#{channel}' - error:", error

      webSockets.push ws

  # Set the termination timeout once we get the 'start' signal
  worker.once 'start', ->
    watch.start()

  workerMessageCount = subCounter * messagesPerChannel

  worker

summaries =
  sub: []
  pub: []

# Master process
runMaster = ->
  channels = [1..channelCount].map -> uuid.v4()
  jobs = []
  workers = []
  summaries = []
  connectedWorkers = 0
  disconnectedWorkers = 0

  # Split up channels between publish workers
  chunks = _.chunk(channels, pubWorkers)
  channelGroups = _.zip(chunks...)

  console.log "[master] Channel Groups:
    #{JSON.stringify(channelGroups)}"
   
  # Add publisher jobs
  [0...pubWorkers].forEach (i) ->
    jobs.push
      id: "pub-#{i}"
      type: "publisher"
      channelGroup: channelGroups[i]
  
  # Set up subscriber workers
  subscribers = [1..subscriberCount]
  chunks = _.chunk(subscribers, subWorkers)
  subscriberGroups = _.zip(chunks...)

  console.log "[master] Subscriber Groups:
    #{JSON.stringify(subscriberGroups)}"

  # Add subscriber jobs
  [0...subWorkers].forEach (i) ->
    jobs.push
      id: "sub-#{i}",
      type: "subscriber"
      channels: channels
      subscriberGroup: subscriberGroups[i]

  console.log "[master] jobs:\n#{JSON.stringify(jobs, null, 2)}"

  # Fork a new worker process for each job
  jobs.forEach (job) ->
    worker = cluster.fork()
    workers.push worker

    # Handle messages from workers
    messageHandler = (msg) ->
      switch msg.action
        # Worker is ready to receive its job
        when 'worker-ready'
          console.log "[master] sending job to worker #{job.id}"
          worker.send
            action: 'job'
            job: job
        # Worker has established all of its WebSockets and is ready to start
        when 'all-ws-connected'
          console.log "[master] worker #{job.id} all ws connected"
          connectedWorkers += 1

          if connectedWorkers == workers.length
            startWorkers = ->
              workers.forEach (worker) ->
                worker.send
                  action: 'start'
            setTimeout startWorkers, initialPublishDelayMillis
        # Worker has terminated all of its WebSockets and is ready to halt
        when 'all-ws-disconnected'
          console.log "[master] worker #{job.id} all ws disconnected"
          disconnectedWorkers += 1

          if disconnectedWorkers == workers.length
            console.log "All workers' WebSockets disconnected."
        # Worker summary supplied
        when 'worker-summary'
          worker.removeListener 'message', messageHandler
          console.log "[master]
            received summary from worker #{job.id}:
            #{JSON.stringify(msg.summary)}"

          summaries.push msg.summary

          if summaries.length == workers.length
            console.log "Summaries:", summaries

            received = summaries
              .filter (sum) -> sum.type == 'subscriber'
              .map ({received}) -> received
              .reduce (v, acc) -> v + acc

            published = summaries
              .filter (sum) -> sum.type == 'publisher'
              .map ({published}) -> published
              .reduce (v, acc) -> v + acc

            console.log "  initial publish delay (ms): #{initialPublishDelayMillis}"
            console.log "          publish delay (ms): #{publishDelayMillis}"
            console.log "                 pub workers: #{pubWorkers}"
            console.log ""
            console.log "                 subscribers: #{subscriberCount}"
            console.log "                 sub workers: #{subWorkers}"
            console.log ""
            console.log "               channel count: #{channelCount}"
            console.log "       messagess per channel: #{messagesPerChannel}"
            console.log ""
            console.log "          total pub messages: #{totalPubMessages}"
            console.log "          total sub messages: #{totalSubMessages}"
            console.log " -------------------------------"
            console.log "             total published: #{published}"
            console.log "              total received: #{received}"

            process.exit 0

    worker.on 'message', messageHandler

  cluster.on 'exit', (worker, code, signal) ->
    console.log "[master] Pub-worker #{worker.process.pid} halted"

# Worker process
runWorker = ->
  console.log "New worker process started with PID #{process.pid}"
  worker = {}

  process.on 'message', (msg) ->
    switch msg.action
      when 'job'
        console.log "[#{msg.job.id}] worker received its job:
          #{JSON.stringify(msg.job)}"

        job = msg.job
        worker.id = msg.job.id
        runSub = () -> runSubWorker job
        runPub = () -> runPubWorker job
        worker.events = if job.type == 'subscriber' then runSub() else runPub()

        worker.events.once 'all-ws-connected', -> 
          process.send
            action: 'all-ws-connected'

        worker.events.once 'all-ws-disconnected', ->
          process.send
            action: 'all-ws-disconnected'

        worker.events.once 'worker-summary', (summary) ->
          process.send
            action: 'worker-summary'
            summary: summary
      when 'start'
        console.log "Starting worker #{worker.id}"
        worker.events.emit 'start'

  process.send
    action: 'worker-ready'

# Entry point
runWorkers = ->
  if cluster.isMaster
    runMaster()
  else
    runWorker()

runWorkers()

