const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mediasoup = require('mediasoup');
const cors = require('cors');

const app = express();
const server = http.createServer(app);

let worker
let router
let transports = []     // [ { socketId1, roomName1, transport, consumer }, ... ]
let producers = []      // [ { socketId1, roomName1, producer, }, ... ]
let consumers = []

const createWorker = async () => {
    worker = await mediasoup.createWorker({
      rtcMinPort: 2000,
      rtcMaxPort: 2020,
    })
    console.log(`worker pid ${worker.pid}`)
  
    worker.on('died', error => {
      // This implies something serious happened, so kill the application
      console.error('mediasoup worker has died')
      setTimeout(() => process.exit(1), 2000) // exit in 2 seconds
    })
  
    return worker
  }

worker = createWorker();

const mediaCodecs = [
    {
      kind: 'audio',
      mimeType: 'audio/opus',
      clockRate: 48000,
      channels: 2,
    },
    {
      kind: 'video',
      mimeType: 'video/VP8',
      clockRate: 90000,
      parameters: {
        'x-google-start-bitrate': 1000,
      },
},];

// Allow CORS for development (adjust as needed)
app.use(cors());

// Create a Socket.IO server and attach it to the HTTP server
const io = new Server(server, {

  cors: {
    origin: '*', // You can restrict this to your frontend domain
    methods: ['GET', 'POST']
  }
});

// Socket.IO connection handler
io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);
  socket.emit('connection-success', {socketId: socket.id})

  socket.on('join-room', async(callback) => {
    if(!router){
        router = await worker.createRouter({ mediaCodecs });
    }
    callback({rtpCapabilities: router.rtpCapabilities})
  });

  socket.on('transport-connect', ({ dtlsParameters }) => {
    console.log('DTLS PARAMS from producer connect... ', { dtlsParameters });
    if(getTransport(socket.id).connected) {
      console.log('already connected');
      return
    };
    getTransport(socket.id).connect({dtlsParameters});
  });

  socket.on('transport-produce', async ({ kind, rtpParameters, appData }, callback) => {
    const producer = await getTransport(socket.id).produce({
      kind,
      rtpParameters,
      paused: false,
    });

    addProducer(producer);

    // if(consumers){
    //     informConsumers(roomName, socket.id, producer.id)
    // }

    producer.on('transportclose', () => {
      console.log('transport for this producer closed ')
      producer.close()
    })

    // Send back to the client the Producer's id
    callback({
      id: producer.id,
      producersExist: producers.length>1 ? true : false
    })
  });

  socket.on('transport-recv-connect', async ({ dtlsParameters, serverConsumerTransportId }) => {
     const consumerTransport = transports.find(transportData => (
      transportData.consumer && transportData.transport.id == serverConsumerTransportId
    )).transport
    
    await consumerTransport.connect({ dtlsParameters })
  });

  socket.on('consume', async ({ 
    rtpCapabilities,
    remoteProducerId,
    serverConsumerTransportId }, callback) => {
    try {
      const consumerTransport = transports.find(transportData => (
        transportData.consumer && transportData.transport.id == serverConsumerTransportId
      )).transport

      // check if the router can consume the specified producer
      if (router.canConsume({
        producerId: remoteProducerId,
        rtpCapabilities
      })) {
        // transport can now consume and return a consumer
        const consumer = await consumerTransport.consume({
          producerId: remoteProducerId,
          rtpCapabilities,
          paused: false,
        })


        consumer.on('transportclose', () => {
          console.log('transport close from consumer')
          
        })

        consumer.on('producerclose', () => {
          console.log('producer of consumer closed')

          socket.emit('producer-closed', { remoteProducerId })

          consumerTransport.close([])
          transports = transports.filter(transportData => transportData.transport.id !== consumerTransport.id)
          consumer.close()
          consumers = consumers.filter(consumerData => consumerData.consumer.id !== consumer.id)
        })

        addConsumer(consumer);

        const params = {
          id: consumer.id,
          producerId: remoteProducerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
        }

        // send the parameters to the client
        callback({ params })
      }
    } catch (error) {
      console.log(error.message)
      callback({
        params: {
          error: error
        }
      })
    }
  })

  socket.on('consumer-resume', async ({ serverConsumerId }) => {
    const { consumer } = consumers.find(consumerData => consumerData.consumer.id === serverConsumerId)
    await consumer.resume();
    
  })

  socket.on('getProducers', callback => {
    let producerList = []
    producers.forEach(producerData => {
      if (producerData.socketId !== socket.id) {
        producerList = [...producerList, producerData.producer.id]
      }
    })

    callback(producerList)
  })

  socket.on('createWebRtcTransport', async({consumer}, callback) => {
    
    createWebRtcTransport(router).then(
      transport => {
        callback({
          params: {
            id: transport.id,
            iceParameters: transport.iceParameters,
            iceCandidates: transport.iceCandidates,
            dtlsParameters: transport.dtlsParameters,
          }
        })

        // add transport to Peer's properties
        addTransport(transport, consumer)
      },
      error => {
        console.log(error)
      })
  });

  const getTransport = (socketId) => {
    const [producerTransport] = transports.filter(transport => transport.socketId === socketId && !transport.consumer)
    return producerTransport.transport
  }
  
  const addTransport = (transport, consumer) => {
    transports = [
      ...transports,
      { socketId: socket.id, transport, consumer, }
    ];
    console.log(transports);
    
  };

  const addProducer = (producer) => {
    producers = [
      ...producers,
      { socketId: socket.id, producer }
    ]
    console.log('all producers: ', producers);
    
  };

  const addConsumer = (consumer) => {
    // add the consumer to the consumers list
    consumers = [
      ...consumers,
      { socketId: socket.id, consumer }
    ];
  };

  const createWebRtcTransport = async (router) => {
    return new Promise(async (resolve, reject) => {
      try {
        // https://mediasoup.org/documentation/v3/mediasoup/api/#WebRtcTransportOptions
        const webRtcTransport_options = {
          listenIps: [
            {
              ip: '0.0.0.0', // replace with relevant IP address
              announcedIp: '10.0.0.115',
            }
          ],
          enableUdp: true,
          enableTcp: true,
          preferUdp: true,
        }
  
        let transport = await router.createWebRtcTransport(webRtcTransport_options)
        console.log(`transport id: ${transport.id}`)
        transport.on('dtlsstatechange', dtlsState => {
          if (dtlsState === 'closed') {
            transport.close()
          }
        })
        transport.on('close', () => {
          console.log('transport closed')
        })
        resolve(transport)
      } catch (error) {
        reject(error)
      }
    })
  }

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
  });
});

// Basic route
app.get('/', (req, res) => {
  res.send('Socket.IO server is running!');
});

// Start the server
const PORT = process.env.PORT || 8000;
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
