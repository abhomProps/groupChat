import { useEffect, useRef, useState } from 'react'
import './App.css'
import {Device} from 'mediasoup-client';
import io from 'socket.io-client';

const socket = io('http://localhost:8000');

function App() {
  let device
  let rtpCapabilities
  let producerTransport
  let consumerTransports = []
  let producer
  let consumer


  const userVideo = useRef(null);
  const [remoteStreams, setRemoteStreams] = useState([]);
  let params = {
    // mediasoup params
  encodings: [
    {
      rid: 'r0', //rtp id
      maxBitrate: 100000, //180kbps
      scalabilityMode: 'S1T3', //dynamically adjust video quality
    },
    {
      rid: 'r1', //rtp id
      maxBitrate: 300000, //360kbps
      scalabilityMode: 'S1T3', //dynamically adjust video quality
    },
    {
      rid: 'r2', //rtp id
      maxBitrate: 900000, //720kbps
      scalabilityMode: 'S1T3', //dynamically adjust video quality
    },
  ],
  // https://mediasoup.org/documentation/v3/mediasoup-client/api/#ProducerCodecOptions
  codecOptions: {
    videoGoogleStartBitrate: 1000
  }
  };

  socket.on('connection-success', ({ socketId }) => {
    console.log('socket id', socketId);
  });

  useEffect(() => {
    const getUserMedia = async() => {
      let localStream;
    
      await navigator.mediaDevices.getUserMedia({ video: true, audio: false })
        .then(stream => {
          if (userVideo.current) {
            userVideo.current.srcObject = stream;
          }
          const track = stream.getVideoTracks()[0];
      
          params = {
            track,
            ...params
          };
  
          localStream = stream;
        })
        .catch(err => {
          console.error("Failed to get media stream:", err);
        });
    
      return () => {
        if (localStream) {
          localStream.getTracks().forEach(track => track.stop());
        }
      };
    }
    getUserMedia()
  }, [])
  

  const joinRoom = async() => {
    socket.emit('join-room', (data) => {
      console.log(`Router RTP Capabilities... ${data.rtpCapabilities}`);

      rtpCapabilities = data.rtpCapabilities;
      createDevice();
    })
  }

  const createDevice = async () => {
    try {
      device = new Device();
      await device.load({
        routerRtpCapabilities: rtpCapabilities
      })
      console.log('device RTP Capabilities', device.rtpCapabilities );
      createSendTransport();
    } catch (error) {
      console.log(error)
      if (error.name === 'UnsupportedError')
        console.warn('browser not supported')
    }
  }

  const createSendTransport = () => {
    socket.emit('createWebRtcTransport', { consumer: false }, ({ params }) => {
      if (params.error) {
        console.log(params.error)
        return
      }
      console.log(params)
      producerTransport = device.createSendTransport(params)

      producerTransport.on('connect', async ({ dtlsParameters }, callback, errback) => {
        try {
          await socket.emit('transport-connect', {
            dtlsParameters,
          })
  
          // Tell the transport that parameters were transmitted.
          callback()
  
        } catch (error) {
          errback(error)
        }
      })
  
      producerTransport.on('produce', async (parameters, callback, errback) => {
        console.log('produce params',parameters)
  
        try {
          await socket.emit('transport-produce', {
            kind: parameters.kind,
            rtpParameters: parameters.rtpParameters,
            appData: parameters.appData,
          }, ({ id, producersExist }) => {
            callback({ id })
            if(producersExist) {
              console.log('there are other producers');
              getProducers();
            }
          })
        } catch (error) {
          errback(error)
        }
      })
  
      connectSendTransport()
    })
  }

  const connectSendTransport = async () => {
    producer = await producerTransport.produce(params);
    
    producer.on('trackended', () => {
      console.log('track ended')
      // close video track
    })
  
    producer.on('transportclose', () => {
      console.log('transport ended');
      // close video track
    })
  }

  const signalNewConsumerTransport = async (remoteProducerId) => {
    await socket.emit('createWebRtcTransport', { consumer: true }, ({ params }) => {
      if (params.error) {
        console.log(params.error)
        return
      }
  
      console.log(params);
      const consumerTransport = device.createRecvTransport(params)
  
      consumerTransport.on('connect', async ({ dtlsParameters }, callback, errback) => {
        try {
          await socket.emit('transport-recv-connect', {
            dtlsParameters,
            serverConsumerTransportId: params.id,
          })
  
          // Tell the transport that parameters were transmitted.
          callback()
        } catch (error) {
          // Tell the transport that something was wrong
          errback(error)
        }
      })
  
      setTimeout(() => {
        connectRecvTransport(consumerTransport, remoteProducerId, params.id)
      },5000)
      
    })
  };

  const connectRecvTransport = async (consumerTransport, remoteProducerId, serverConsumerTransportId) => {
    await socket.emit('consume', {
      rtpCapabilities: device.rtpCapabilities,
      remoteProducerId,
      serverConsumerTransportId
    }, async ({ params }) => {
      if (params.error) {
        console.log('Cannot Consume')
        return
      }
  
      console.log('consumer params', params)
      const consumer = await consumerTransport.consume({
        id: params.id,
        producerId: params.producerId,
        kind: params.kind,
        rtpParameters: params.rtpParameters
      })
  
      consumerTransports = [
        ...consumerTransports,
        {
          consumerTransport,
        serverConsumerTransportId: params.id,
        producerId: remoteProducerId,
        consumer
        }
      ];
  
      // Destructure and retrieve the video track from the producer
      const { track } = consumer;
      console.log('track: ', track);
      
      const stream = new MediaStream();
      stream.addTrack(track);

      setRemoteStreams(prev => [...prev, stream]);
    
      socket.emit('consumer-resume', {serverConsumerId: params.id});
      
    })
  }

  const getProducers = () => {
    socket.emit('getProducers', producerIds => {
      console.log('all producers', producerIds);
      producerIds.forEach(signalNewConsumerTransport)
    })
  }

  
  return (
    <div style={{
      width: '90vw',
      height: '100vh',
      display: 'flex',
      flexDirection: 'column',
      padding: '16px',
      boxSizing: 'border-box',
    }}>
      {/* Streams Grid */}
      <div style={{
        flex: 1,
        display: 'grid',
        gridTemplateColumns: 'repeat(4, 1fr)',
        gap: '16px',
        overflowY: 'auto',
      }}>
        {/* Stream Divs */}
        <div style={{ backgroundColor: 'lightblue', height: '280px', borderRadius: '8px', padding: 4 }}>
          <video
            ref={userVideo}
            autoPlay
            muted
            playsInline
            style={{ width: 320, backgroundColor: 'gray', borderRadius: '8px' }}
          />
        </div>
        {remoteStreams.map((stream, index) => (
  <div key={index} style={{ backgroundColor: 'purple', height: '200px', borderRadius: '8px' }}>
    <video
      autoPlay
      playsInline
      muted={false}
      ref={el => {
        if (el) el.srcObject = stream;
      }}
      style={{ width: 320, backgroundColor: 'gray', borderRadius: '8px' }}
    />
  </div>
))}

      </div>
    
      {/* Join Room Button */}
      <button style={{
        padding: '12px 24px',
        fontSize: '16px',
        backgroundColor: 'white',
        color: 'red',
        border: 'none',
        borderRadius: '8px',
        cursor: 'pointer',
        alignSelf: 'center',
      }} onClick={joinRoom}>
        Join Room
      </button>
    </div>
    
    
  )
}

export default App
