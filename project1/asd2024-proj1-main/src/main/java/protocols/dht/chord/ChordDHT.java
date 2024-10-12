package protocols.dht.chord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.AutomatedApp;
import protocols.dht.chord.messages.FindSuccessorMessage;
import protocols.dht.chord.messages.FoundSuccessorMessage;
import protocols.dht.chord.notifications.TCPChannelCreatedNotification;
import protocols.dht.chord.requests.LookupRequest;
import protocols.dht.chord.replies.LookupReply;
import protocols.dht.chord.timers.RetryTCPConnectionsTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.AuxCalcs;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class ChordDHT extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(ChordDHT.class);

	public static final short PROTOCOL_ID = 500;
	public static final String PROTOCOL_NAME = "ChordDHT";

	private final short COMM_PROTOCOL_ID;

	private final int tcpChannelId;
	private final Set<Host> pendingHostConnections;

	private ChordNode predecessorNode;
	private final ChordNode thisNode;
	private final Finger[] fingers;

	private final Set<LookupRequest> pendingLookupRequests;

	public ChordDHT(Properties properties, Host thisHost, short commProtocolID) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);

		COMM_PROTOCOL_ID = commProtocolID;

		pendingHostConnections = new HashSet<>();
		pendingLookupRequests = new HashSet<>();

		//initialize thisNode and predecessorNode
		String myPeerIDHex = properties.getProperty(AutomatedApp.PROPERTY_NODE_ID);
		BigInteger myPeerID = new BigInteger(myPeerIDHex, 16);
		thisNode = new ChordNode(myPeerID, thisHost);
		predecessorNode = thisNode;

		//initialize fingers
		int numFingers = AuxCalcs.log2Ceil(Integer.parseInt(properties.getProperty("id_bits")));
		fingers = new Finger[numFingers];
		BigInteger fingerEnd = thisNode.getPeerID().add(BigInteger.valueOf(2).pow(fingers.length)).mod(BigInteger.valueOf(2).pow(fingers.length));
		for (int i = fingers.length-1; i >= 0; i--) {
			//(thisNode.getPeerID() + 2^i) mod 2^fingers.length
			BigInteger fingerStart = thisNode.getPeerID().add(BigInteger.valueOf(2).pow(i)).mod(BigInteger.valueOf(2).pow(fingers.length));
			fingers[i] = new Finger(fingerStart, fingerEnd, thisNode);
			fingerEnd = fingerStart;
		}

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
		registerMessageSerializer(tcpChannelId, FindSuccessorMessage.MSG_ID, FindSuccessorMessage.serializer);
		registerMessageSerializer(tcpChannelId, FoundSuccessorMessage.MSG_ID, FoundSuccessorMessage.serializer);

		//register message handlers
		registerMessageHandler(tcpChannelId, FindSuccessorMessage.MSG_ID, this::uponFindSuccessorMessage, this::uponMessageFail);
		registerMessageHandler(tcpChannelId, FoundSuccessorMessage.MSG_ID, this::uponFoundSuccessorMessage, this::uponMessageFail);

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
			while (isSoloNode());
		}
	}

	private void connectToHost(String contact) {
		try {
			String[] hostElems = contact.split(":");
			Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
			//We add to the pending set until the connection is successful
			pendingHostConnections.add(contactHost);
			openConnection(contactHost);
		} catch (Exception e) {
			logger.error("Invalid contact on configuration: " + contact);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private void joinNetwork(Host host) {
		initializeFingerTable(host);
		updateOtherNodes();
		moveKeysFromSuccessor();
	}

	private void initializeFingerTable(Host host) {

	}

	private void updateOtherNodes() {

	}

	private void moveKeysFromSuccessor() {

	}

	private void processDisconnection(Host host) {

	}

	private boolean isSoloNode() {
		return fingers[0].getChordNode() == thisNode;
	}

	private ChordNode closestPrecedingNode(BigInteger peerID) {
		for (int i = fingers.length-1; i >= 0 ; i--) {
			if (fingers[i].isInInterval(peerID)) {
				return fingers[i].getChordNode();
			}
		}
		return thisNode;
	}

	/*--------------------------------- Requests ---------------------------------------- */

	private void uponLookupRequest(LookupRequest request, short protoID) {
		logger.info("Received LookupRequest: " + request.toString());

		FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(request.getMid(), thisNode.getHost(), thisNode.getHost(), PROTOCOL_ID, request.getPeerIDNumerical());
		uponFindSuccessorMessage(findSuccessorMessage, thisNode.getHost(), PROTOCOL_ID, tcpChannelId);
	}

	/*--------------------------------- Messages ---------------------------------------- */

	private void uponFindSuccessorMessage(FindSuccessorMessage findSuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received LookupMessage: " + findSuccessorMessage.toString());

		if (Finger.belongsToSuccessor(thisNode.getPeerID(), fingers[0].getChordNode().getPeerID(), findSuccessorMessage.getKey())) {
			FoundSuccessorMessage foundSuccessorMessage = new FoundSuccessorMessage(findSuccessorMessage, thisNode);
			sendMessage(foundSuccessorMessage, foundSuccessorMessage.getOriginalSender());
			return;
		}

		ChordNode closestPrecedingNode = closestPrecedingNode(findSuccessorMessage.getKey());
		FindSuccessorMessage findSuccessorMessage2 = new FindSuccessorMessage(findSuccessorMessage, thisNode.getHost());
		sendMessage(findSuccessorMessage2, closestPrecedingNode.getHost());
	}

	private void uponFoundSuccessorMessage(FoundSuccessorMessage foundSuccessorMessage, Host from, short sourceProto, int channelId) {
		logger.info("Received FoundSuccessorMessage: " + foundSuccessorMessage.toString());

		LookupReply lookupReply = new LookupReply(foundSuccessorMessage);
		lookupReply.addElementToPeers(foundSuccessorMessage.getSenderPeerID().toByteArray(), foundSuccessorMessage.getSender());
		sendReply(lookupReply, COMM_PROTOCOL_ID);
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
		Host peer = event.getNode();
		logger.debug("Connection to {} is up", peer);
		pendingHostConnections.remove(peer);

		if (this.isSoloNode()) {
			joinNetwork(peer);
		}
	}

	//If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
	//protocol. Alternatively, we could do smarter things like retrying the connection X times.
	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is down cause {}", peer, event.getCause());
		processDisconnection(peer);
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
