package protocols.apps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.timers.ExitTimer;
import protocols.apps.timers.SendMessageTimer;
import protocols.apps.timers.StartTimer;
import protocols.apps.timers.StopTimer;
import protocols.point2point.notifications.Deliver;
import protocols.point2point.requests.Send;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import utils.HashProducer;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AutomatedApp extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(AutomatedApp.class);

	//Protocol information, to register in babel
	public static final String PROTO_NAME = "AutomateCommunicationApp";
	public static final short PROTO_ID = 300;

	private final short commProtoId;

	//Size of the payload of each message (in bytes)
	private final int payloadSize;
	//Time to wait until starting sending messages
	private final int prepareTime;
	//Time to run before shutting down
	private final int runTime;
	//Time to wait until starting sending messages
	private final int cooldownTime;
	//Interval between each broadcast
	private final int broadcastInterval;
	//random seed for topic generation
	private final int randomSeed;
	//Number of different peers to be considered
	private final int nPeers;
	//Number of bits used for peer ID's
	private final int idBits;

	private final int processSequenceNumber;

	private long sendMessageTimer;

	private final ArrayList<String> peerIDsHex;
	private final ArrayList<byte[]> peerIDs;
	private final byte[] myPeerID;
	private final String myPeerIDHex;

	private Random r;

	public static final String PROPERTY_NODE_ID = "node_opaque_id";

	public AutomatedApp(Properties properties, short commProtoId) throws HandlerRegistrationException {
		super(PROTO_NAME, PROTO_ID);
		this.commProtoId = commProtoId;

		//Read configurations
		this.payloadSize = Integer.parseInt(properties.getProperty("payload_size"));
		this.prepareTime = Integer.parseInt(properties.getProperty("prepare_time")); //in seconds
		this.cooldownTime = Integer.parseInt(properties.getProperty("cooldown_time")); //in seconds
		this.runTime = Integer.parseInt(properties.getProperty("run_time")); //in seconds
		this.broadcastInterval = Integer.parseInt(properties.getProperty("broadcast_interval")); //in milliseconds
		this.randomSeed = Integer.parseInt(properties.getProperty("random_seed", "10000"));
		this.nPeers = Integer.parseInt(properties.getProperty("n_peers"));
		this.idBits = Integer.parseInt(properties.getProperty("id_bits"));

		this.peerIDsHex = new ArrayList<>(nPeers);
		this.peerIDs = new ArrayList<>(nPeers);

		//Generate Topics
		r = new Random(this.randomSeed);
		for (int i = 0; i < this.nPeers; i++) {
			byte[] value = new byte[100];
			r.nextBytes(value);
			byte[] id = HashProducer.hashValue("Node" + i + Arrays.toString(value), this.idBits);
			this.peerIDs.add(id);
			this.peerIDsHex.add(HashProducer.toNumberFormat(id).toString(16));
		}

		if (properties.containsKey("processSequence")) {
			this.processSequenceNumber = Integer.parseInt(properties.getProperty("processSequence"));
			this.myPeerID = this.peerIDs.get(this.processSequenceNumber - 1);
			this.myPeerIDHex = this.peerIDsHex.get(this.processSequenceNumber - 1);
			properties.put(PROPERTY_NODE_ID, this.myPeerIDHex);
			logger.info("My peer ID: {}", new BigInteger(1, this.myPeerID));
		} else {
			this.processSequenceNumber = -1;
			this.myPeerID = null;
			this.myPeerIDHex = null;
			System.err.println("You must indicate the sequence number of the process [1,npeers] using the property 'processSequence'");
			System.exit(1);
		}

		r = new Random(HashProducer.randomInitializer(myPeerID));

		//Setup handlers
		subscribeNotification(Deliver.NOTIFICATION_ID, this::uponDeliver);

		registerTimerHandler(SendMessageTimer.TIMER_ID, this::uponSendMessageTimer);
		registerTimerHandler(StartTimer.TIMER_ID, this::uponStartTimer);
		registerTimerHandler(StopTimer.TIMER_ID, this::uponStopTimer);
		registerTimerHandler(ExitTimer.TIMER_ID, this::uponExitTimer);

	}

	@Override
	public void init(Properties props) {
		//Wait prepareTime seconds before starting
		logger.info("Waiting...");
		setupTimer(new StartTimer(), prepareTime * 1000L);
	}

	private void uponStartTimer(StartTimer startTimer, long timerId) {
		logger.info("Starting publications");
		//Start broadcasting periodically
		sendMessageTimer = setupPeriodicTimer(new SendMessageTimer(), 0, broadcastInterval);
		//And setup the stop timer
		setupTimer(new StopTimer(), runTime * 1000L);
	}

	private void uponSendMessageTimer(SendMessageTimer sendMessageTimer, long timerId) {
		//Upon triggering the broadcast timer, create a new message
		String toSend = randomCapitalLetters(Math.max(0, payloadSize));
		//ASCII encodes each character as 1 byte
		byte[] payload = toSend.getBytes(StandardCharsets.US_ASCII);

		byte[] destination = null;
		while (destination == null) {
			int d = r.nextInt(this.peerIDs.size());
			if (d != this.processSequenceNumber - 1) {
				destination = this.peerIDs.get(d);
			}
		}

		UUID mid = UUID.randomUUID();
		Send send = new Send(this.myPeerID, destination, mid, payload);

		logger.info("Sending: {} -> {} : {} ({})", this.myPeerIDHex, HashProducer.toNumberFormat(destination).toString(16), mid.toString(), payload.length);
		//And send it to the dissemination protocol
		sendRequest(send, commProtoId);
	}

	private void uponDeliver(Deliver message, short sourceProto) {
		//Upon receiving a message, simply print it
		logger.info("Received {} -> {} - {} ({})", HashProducer.toNumberFormat(message.getSenderPeerID()).toString(16), this.myPeerIDHex, message.getMessageID().toString(), message.getMessagePayload().length);
	}

	private void uponStopTimer(StopTimer stopTimer, long timerId) {
		logger.info("Stopping publications");
		this.cancelTimer(sendMessageTimer);
		setupTimer(new ExitTimer(), cooldownTime * 1000L);
	}

	private void uponExitTimer(ExitTimer exitTimer, long timerId) {
		logger.info("Exiting...");
		System.exit(0);
	}

	public static String randomCapitalLetters(int length) {
		int leftLimit = 65; // letter 'A'
		int rightLimit = 90; // letter 'Z'
		Random random = new Random();
		return random.ints(leftLimit, rightLimit + 1).limit(length)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
	}
}
