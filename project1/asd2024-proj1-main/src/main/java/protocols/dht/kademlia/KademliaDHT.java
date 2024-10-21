package protocols.dht.kademlia;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.AutomatedApp;
import protocols.dht.kademlia.messages.*;
import protocols.dht.kademlia.notifications.TCPChannelCreatedNotification;
import protocols.dht.kademlia.requests.LookupRequest;
import protocols.dht.kademlia.timers.RetryTCPConnectionsTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class KademliaDHT extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(KademliaDHT.class);

	public static final short PROTOCOL_ID = 500;
	public static final String PROTOCOL_NAME = "KademliaDHT";
	private final short COMM_PROTOCOL_ID;

	private final int tcpChannelId;
	private final Set<Host> pendingHostConnections;
	private final Set<UUID> pendingLookupRequests;
	private final KademliaTree bucket;

	public KademliaDHT(Properties properties, Host thisHost, short commProtocolID) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);

		COMM_PROTOCOL_ID = commProtocolID;

		pendingHostConnections = new HashSet<>();
		pendingLookupRequests = new HashSet<>();
		String myPeerIDHex = properties.getProperty(AutomatedApp.PROPERTY_NODE_ID);
		BigInteger myPeerID = new BigInteger(myPeerIDHex, 16);
		bucket = new KademliaTree(new KademliaNode(myPeerID, thisHost));

		//register TCP channel
		Properties tcpChannelProperties = new Properties();
		tcpChannelProperties.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); //The address to bind to
		tcpChannelProperties.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port")); //The port to bind to
		tcpChannelProperties.setProperty(TCPChannel.METRICS_INTERVAL_KEY, properties.getProperty("channel_metrics_interval", "10000")); //The interval to receive channel metrics
		tcpChannelProperties.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
		tcpChannelProperties.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
		tcpChannelProperties.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
		tcpChannelId = createChannel(TCPChannel.NAME, tcpChannelProperties); //Create the channel with the given properties

		//register TCP channel events
		registerChannelEventHandler(tcpChannelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(tcpChannelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(tcpChannelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
		registerChannelEventHandler(tcpChannelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(tcpChannelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

		//register request handlers
		registerRequestHandler(LookupRequest.REQUEST_ID, this::uponLookupRequest);

		//register message serializers
		registerMessageSerializer(tcpChannelId, FindNodeMessage.MSG_ID, FindNodeMessage.serializer);
		registerMessageSerializer(tcpChannelId, FindValueMessage.MSG_ID, FindValueMessage.serializer);
		registerMessageSerializer(tcpChannelId, PingMessage.MSG_ID, PingMessage.serializer);
		registerMessageSerializer(tcpChannelId, PongMessage.MSG_ID, PongMessage.serializer);
		registerMessageSerializer(tcpChannelId, KBucketMessage.MSG_ID, KBucketMessage.serializer);
		registerMessageSerializer(tcpChannelId, ValueMessage.MSG_ID, ValueMessage.serializer);
		registerMessageSerializer(tcpChannelId, StoreKeyValueMessage.MSG_ID, StoreKeyValueMessage.serializer);

		//register message handlers
		registerMessageHandler(tcpChannelId, FindNodeMessage.MSG_ID, this::uponFindNodeMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, FindValueMessage.MSG_ID, this::uponFindValueMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, PingMessage.MSG_ID, this::uponPingMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, PongMessage.MSG_ID, this::uponPongMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, KBucketMessage.MSG_ID, this::uponKBucketMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, ValueMessage.MSG_ID, this::uponValueMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, StoreKeyValueMessage.MSG_ID, this::uponStoreKeyValueMessage, this::uponMessageFail);

		//register timer handlers
		registerTimerHandler(RetryTCPConnectionsTimer.TIMER_ID, this::retryTCPConnections);
	}

	@Override
	public void init(Properties props) {
		//inform the point2point algorithm above about the TCP channel to use
		triggerNotification(new TCPChannelCreatedNotification(tcpChannelId));

		//initiate timers
		setupPeriodicTimer(new RetryTCPConnectionsTimer(), 1000, 1000);

		//establish TCP connection to contact host
		if (props.containsKey("contact")) {
			connectToHost(props.getProperty("contact"));
		}
	}

	private void connectToHost(String contact) {
		try {
			String[] hostElems = contact.split(":");
			Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
			pendingHostConnections.add(contactHost);
			openConnection(contactHost);
		} catch (Exception e) {
			logger.error("Invalid contact on configuration: " + contact);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponLookupRequest(LookupRequest request, short protoID) {
		logger.info("Received LookupRequest: " + request.toString());

		FindNodeMessage findNodeMessage = new FindNodeMessage(request.getMid(), bucket.getThisNode(), request.getPeerIDNumerical());
		uponFindNodeMessage(findNodeMessage, bucket.getThisNode().getHost(), protoID, tcpChannelId);
		pendingLookupRequests.add(request.getMid());
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponFindNodeMessage(FindNodeMessage findNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received FindNodeMessage: " + findNodeMessage.toString());
	}

	private void uponFindValueMessage(FindValueMessage findNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received FindValueMessage: " + findNodeMessage.toString());
	}

	private void uponKBucketMessage(KBucketMessage findNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received KBucketMessage: " + findNodeMessage.toString());
	}

	private void uponPingMessage(PingMessage findNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received PingMessage: " + findNodeMessage.toString());
	}

	private void uponPongMessage(PongMessage findNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received PongMessage: " + findNodeMessage.toString());
	}

	private void uponStoreKeyValueMessage(StoreKeyValueMessage findNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received StoreKeyValueMessage: " + findNodeMessage.toString());
	}

	private void uponValueMessage(ValueMessage findNodeMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received ValueMessage: " + findNodeMessage.toString());
	}

	private void uponMessageFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	/*--------------------------------- Timers ---------------------------------------- */

	private void retryTCPConnections(RetryTCPConnectionsTimer timer, long timerId) {
		logger.debug("retryTCPConnections: {}", pendingHostConnections);
		for (Host host : pendingHostConnections) {
			openConnection(host);
		}
	}

	/* --------------------------------- TCPChannel Events ---------------------------- */

	//If a connection is successfully established, this event is triggered. In this protocol, we want to add the
	//respective peer to the membership, and inform the Dissemination protocol via a notification.
	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		Host peerHost = event.getNode();
		logger.debug("Connection to {} is up", peerHost);
		pendingHostConnections.remove(peerHost);
	}

	//If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
	//protocol. Alternatively, we could do smarter things like retrying the connection X times.
	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is down cause {}", peer, event.getCause());
	}

	//If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
	//pending set. Note that this event is only triggered while attempting a connection, not after connection.
	//Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
	}

	//If someone established a connection to me, this event is triggered. In this protocol we do nothing with this event.
	//If we want to add the peer to the membership, we will establish our own outgoing connection.
	// (not the smartest protocol, but its simple)
	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		logger.trace("Connection from {} is up", event.getNode());
	}

	//A connection someone established to me is disconnected.
	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
	}

}
