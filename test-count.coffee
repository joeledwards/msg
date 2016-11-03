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
elixirCluster =
  type: 'elixir'
  uris: [
    'ws://172.20.0.2:8080/pubsub'
    'ws://172.20.0.3:8080/pubsub'
    'ws://172.20.0.4:8080/pubsub'
    #'ws://172.20.0.5:8080/pubsub'
    #'ws://172.20.0.6:8080/pubsub'
    #'ws://172.20.0.7:8080/pubsub'
    #'ws://172.20.0.8:8080/pubsub'
    #'ws://172.20.0.9:8080/pubsub'
  ]

# Node.js Servers
nodeCluster =
  type: 'nodejs'
  uris: [
    'ws://172.18.0.4:8888'
    'ws://172.18.0.5:8888'
    'ws://172.18.0.6:8888'
    'ws://172.18.0.7:8888'
    'ws://172.18.0.8:8888'
    'ws://172.18.0.9:8888'
    'ws://172.18.0.10:8888'
    'ws://172.18.0.11:8888'
  ]

pubSubCluster = elixirCluster
uris = pubSubCluster.uris
uriCount = uris.length

# Publisher Tuning Parameters
initialPublishDelayMillis = 5000 # Delay between all sockets connected and first message published
publishDelayMillis = 100 # Delay between message sends on each channel
messagesPerChannel = 1000 # Number of messages to send to each channel
channelCount = 100 # Number of distinct channels
pubWorkers = 2 # Channels are divied up between pub-workers

# Subscriber Tuning Parameters
subscriberChannelTimeoutMillis = 10000
subscriberCount = 10 # Each subscriber connects to all channels
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
    channels: {}
    workerDuration: 0
    workerStart: 0
    workerEnd: 0

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
    channelStart = 0
    channelEnd = 0

    ws.once 'open', ->
      didConnect = true
      active += 1

      console.log "[#{id}] channel '#{channel}' connected (#{active} of #{totalWs}) : #{uri}"

      if active == totalWs
        process.nextTick -> worker.emit('all-ws-connected')

      sendMessage = () ->
        try
          # Check if channel is done
          if channelSendCount >= messagesPerChannel
            channelWatch.stop()
            channelEnd = moment.utc()
            console.log "[#{id}] All #{messagesPerChannel} messages published to channel '#{channel}' in #{channelWatch}"
            summary.channels[channel] =
              duration: channelWatch.duration().nanos()
              start: channelStart
              end: channelEnd

            # Check if worker is done
            if workerSendCount >= workerMessageCount
              watch.stop()
              summary.workerEnd = moment.utc()
              console.log "[#{id}] All #{workerMessageCount} messages published in #{watch} : #{uri}"
              summary.workerDuration = watch.duration().nanos()
              summary.published = workerSendCount

            ws.close()

          # While channel is not done, continue sending messages
          else
            record =
              action: 'publish'
              channel: channel
              message:
                uuid: uuid.v4()
            message = JSON.stringify record

            ws.send message
            workerSendCount += 1
            channelSendCount += 1

            timedSummary()

            setTimeout sendMessage, publishDelayMillis

        catch error
          console.error "[#{id}] Error publishing message:", error

      worker.once 'start', ->
        if summary.workerStart == 0
          watch.start()
          summary.workerStart = moment.utc()

        channelWatch.start()
        channelStart = moment.utc()
        console.log "[#{id}] Publishing first message to channel '#{channel}'"
        sendMessage()

    ws.once 'close', ->
      if didConnect
        active -= 1

      console.log "[#{id}] channel '#{channel}' disconnected (#{active} of #{totalWs}) : #{uri}"
      closed = true

      if active < 1
        finalSummary()
        process.nextTick -> worker.emit('all-ws-disconnected')
        process.nextTick -> worker.emit('worker-summary', summary)

    ws.on 'error', (error) ->
      console.error "[#{id}] channel '#{channel}' - error:", error

    webSockets.push ws

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
    channels: {}
    workerDuration: 0
    workerStart: 0
    workerEnd: 0

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
      lastReceived = 0

      channelRecvCount = 0
      didConnect = false
      channelWatch = durations.stopwatch()
      channelStart = 0
      channelEnd = 0

      closeHandler = ->
        if didConnect
          active -= 1

        console.log "[#{id}] Subscriber #{i} to channel '#{channel}' disconnected (#{active} of #{totalWs}) : #{uri}"

        if active < 1
          finalSummary()
          process.nextTick -> worker.emit('all-ws-disconnected')
          process.nextTick -> worker.emit('worker-summary', summary)

      # Wrap up this WebSocket
      wrappedUp = false
      wrapUp = (unsubscribe) ->
        console.log "Wrap up called: wrappedUp=#{wrappedUp} unsubscribe=#{unsubscribe}"

        if wrappedUp == false
          wrappedUp = true

          channelWatch.stop()
          channelEnd = moment.utc()
          console.log "[#{id}] #{channelRecvCount} messages consumed from channel '#{channel}' in #{channelWatch}"
          summary.channels[channel] =
            duration: channelWatch.duration().nanos()
            start: channelStart
            end: channelEnd

          # Check if worker is done
          if workerRecvCount >= workerMessageCount
            watch.stop()
            summary.workerEnd = moment.utc()
            console.log "[#{id}] All #{workerMessageCount} messages consumed in #{watch}"
            summary.workerDuration = watch.duration().nanos()
            summary.received = workerRecvCount

          if unsubscribe == true
            try
              ws.send JSON.stringify({
                action: 'unsubscribe'
                channel: channel
              })
            catch error
              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' : Error unsubscribing:", error
          else
            try
              ws.close()
            catch error
              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' : Error closing WebSocket:", error
        

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
          # Handle subscription response
          when 'subscribe'
            if record.result == 'success'
              didConnect = true
              active += 1

              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' connected (#{active} of #{totalWs}) : #{uri}"

              if active >= totalWs
                process.nextTick -> worker.emit('all-ws-connected')
            else
              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' failed to subscribe"
              ws.close()

          # Handle unsubscription response
          when 'unsubscribe'
            if record.result != 'success'
              console.log "[#{id}] Subscriber #{i} to channel '#{channel}' failed to unsubscribe"

            ws.close()

          # Handle received messages
          when 'message'
            lastReceived = moment.utc()

            if channelStart == 0
              channelWatch.start()
              channelStart = moment.utc()

              if summary.workerStart == 0
                watch.start()
                summary.workerStart = moment.utc()

            workerRecvCount += 1
            channelRecvCount += 1

            # Check if channel is done
            if channelRecvCount >= messagesPerChannel
              wrapUp(true)

            # While channel is not, keep reporting progress
            else
              timedSummary()

          else
            console.error "Unregonized message from pub/sub server!"

      # The WebSocket has been closed
      ws.once 'close', closeHandler

      ws.on 'error', (error) ->
        console.error "[#{id}] Subscriber #{i} to channel '#{channel}' - error:", error

      webSockets.push ws

      # If we have gone too long since the previous message was received, halt.
      timeoutCheck = ->
        now = moment.utc()

        if lastReceived > 0 and (now - lastReceived) > subscriberChannelTimeoutMillis
          console.log "[#{id}] Subscriber #{i} to channel '#{channel}' timeout after #{subscriberChannelTimeoutMillis} ms"
          wrapUp(false)
        else
          setTimeout timeoutCheck, 1000

      # Check every second
      setTimeout timeoutCheck, 1000
        
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
            console.log "Summaries:", JSON.stringify(summaries, null, 2)

            pubSummaries = summaries
            .filter (sum) -> sum.type == 'publisher'

            subSummaries = summaries
            .filter (sum) -> sum.type == 'subscriber'

            published = pubSummaries
            .map ({published}) -> published
            .reduce (v, acc) -> v + acc

            received = subSummaries
            .map ({received}) -> received
            .reduce (v, acc) -> v + acc

            pubWorkerDurations = pubSummaries.map ({workerDuration}) -> workerDuration
            meanPubWorkerDuration = pubWorkerDurations.reduce((v, acc) -> v + acc) / pubWorkerDurations.length
            maxPubWorkerDuration = pubWorkerDurations.reduce (v, m) -> Math.max(v, m)
            minPubWorkerDuration = pubWorkerDurations.reduce (v, m) -> Math.min(v, m)

            subWorkerDurations = subSummaries.map ({workerDuration}) -> workerDuration
            meanSubWorkerDuration = subWorkerDurations.reduce((v, acc) -> v + acc) / subWorkerDurations.length
            maxSubWorkerDuration = subWorkerDurations.reduce (v, m) -> Math.max(v, m)
            minSubWorkerDuration = subWorkerDurations.reduce (v, m) -> Math.min(v, m)

            pubChannelDurations = _(pubSummaries)
              .map ({channels}) ->
                _(channels)
                .values()
                .map ({duration}) -> duration
                .value()
              .flatten()
              .value()
            meanPubChannelDuration = pubChannelDurations.reduce((v, acc) -> v + acc) / pubChannelDurations.length
            maxPubChannelDuration = pubChannelDurations.reduce (v, m) -> Math.max(v, m)
            minPubChannelDuration = pubChannelDurations.reduce (v, m) -> Math.min(v, m)

            subChannelDurations = _(subSummaries)
              .map ({channels}) ->
                _(channels)
                .values()
                .map ({duration}) -> duration
                .value()
              .flatten()
              .value()
            meanSubChannelDuration = subChannelDurations.reduce((v, acc) -> v + acc) / subChannelDurations.length
            maxSubChannelDuration = subChannelDurations.reduce (v, m) -> Math.max(v, m)
            minSubChannelDuration = subChannelDurations.reduce (v, m) -> Math.min(v, m)

            # Channel duration summary
            console.log "Channel Durations"

            console.log "  Publishers"
            pubSummaries
            .forEach ({id, channels}) ->
              console.log "    Worker #{id}"
              _(channels)
              .toPairs()
              .each ([channel, {duration, start, end}]) ->
                console.log "      #{channel} : #{durations.duration(duration)} 
                  (#{moment(start).toISOString()} - #{moment(end).toISOString()})"

            console.log "  Subscribers"
            subSummaries
            .forEach ({id, channels}) ->
              console.log "    Worker #{id}"
              _(channels)
              .toPairs()
              .each ([channel, {duration, start, end}]) ->
                console.log "      #{channel} : #{durations.duration(duration)} 
                  (#{moment(start).toISOString()} - #{moment(end).toISOString()})"

            console.log ""
            console.log ""

            # Worker duration summary
            console.log "Worker Durations"

            console.log "  Publishers"
            pubSummaries
            .forEach ({id, workerDuration, workerStart, workerEnd}) ->
              console.log "    #{id} : #{durations.duration(workerDuration)}
                (#{moment(workerStart).toISOString()} - #{moment(workerEnd).toISOString()})"

            console.log "  Subscribers"
            subSummaries
            .forEach ({id, workerDuration, workerStart, workerEnd}) ->
              console.log "    #{id} : #{durations.duration(workerDuration)}
                (#{moment(workerStart).toISOString()} - #{moment(workerEnd).toISOString()})"

            console.log ""
            console.log ""

            # General summary
            console.log " ----- GENERAL -----"
            console.log "                Cluster type: #{pubSubCluster.type}"
            console.log "                Cluster size: #{uriCount}"
            console.log "               Channel count: #{channelCount}"
            console.log "       Messagess per channel: #{messagesPerChannel}"
            console.log ""
            console.log " ----- PUBLISHERS -----"
            console.log "  Initial publish delay (ms): #{initialPublishDelayMillis}"
            console.log "  Channel publish delay (ms): #{publishDelayMillis}"
            console.log "                 Pub workers: #{pubWorkers}"
            console.log ""
            console.log "    min Pub channel duration: #{durations.duration(minPubChannelDuration)}"
            console.log "   mean Pub channel duration: #{durations.duration(meanPubChannelDuration)}"
            console.log "    max Pub channel duration: #{durations.duration(maxPubChannelDuration)}"
            console.log ""
            console.log "     min Pub worker duration: #{durations.duration(minPubWorkerDuration)}"
            console.log "    mean Pub worker duration: #{durations.duration(meanPubWorkerDuration)}"
            console.log "     max Pub worker duration: #{durations.duration(maxPubWorkerDuration)}"
            console.log ""
            console.log " ----- SUBSCRIBERS -----"
            console.log "                 Subscribers: #{subscriberCount}"
            console.log "                 Sub workers: #{subWorkers}"
            console.log ""
            console.log "    min Sub channel duration: #{durations.duration(minSubChannelDuration)}"
            console.log "   mean Sub channel duration: #{durations.duration(meanSubChannelDuration)}"
            console.log "    max Sub channel duration: #{durations.duration(maxSubChannelDuration)}"
            console.log ""
            console.log "     min Sub worker duration: #{durations.duration(minSubWorkerDuration)}"
            console.log "    mean Sub worker duration: #{durations.duration(meanSubWorkerDuration)}"
            console.log "     max Sub worker duration: #{durations.duration(maxSubWorkerDuration)}"
            console.log ""
            console.log " -------------------------------"
            console.log "       Expected Pub messages: #{totalPubMessages}"
            console.log "       Expected Sub messages: #{totalSubMessages}"
            console.log ""
            console.log "         Actual Pub messages: #{published}"
            console.log "         Actual Sub messages: #{received}"

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

