package etape3.composants;

import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape2.endpoints.DHTServicesEndPoint;
import etape3.CVM;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.utils.aclocks.AcceleratedClock;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerConnector;
import fr.sorbonne_u.utils.aclocks.ClocksServerOutboundPort;

/**
 * ClientBCM est un composant client qui interagit avec le service DHT pour
 * effectuer des opérations de type MapReduce, ainsi que des opérations CRUD sur
 * les données stockées dans le DHT.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ClientBCM extends AbstractComponent implements DHTServicesI{

	protected DHTServicesEndPoint endPointClientFacade; // Point d'accès aux services DHT

	protected AcceleratedClock dhtClock; // Référence à l'horloge

	private static final int SCHEDULABLE_THREADS = 1;

	private static final int THREADS_NUMBER = 0;

	ClocksServerOutboundPort p;

	/**
	 * Constructeur du composant ClientBCM.
	 * 
	 * @param uri                  URI du composant.
	 * @param endpointClientFacade Point d'accès aux services DHT.
	 */
	protected ClientBCM(String uri, DHTServicesEndPoint endpointClientFacade) {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.endPointClientFacade = endpointClientFacade;

	}

	/**
	 * La connexion au serveur horloge.
	 *
	 * @throws Exception the exception
	 */
	protected void connectToClockServer() throws Exception {
		p = new ClocksServerOutboundPort(this);
		p.publishPort();

		this.doPortConnection(p.getPortURI(), ClocksServer.STANDARD_INBOUNDPORT_URI,
				ClocksServerConnector.class.getCanonicalName());

		this.dhtClock = p.getClock(CVM.TEST_CLOCK_URI);

		this.logMessage("En attente du démarrage de l'horloge...");
		if (dhtClock.startTimeNotReached()) {
			dhtClock.waitUntilStart();
		}
		this.logMessage("Horloge démarrée : " + dhtClock.getStartInstant());
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#get(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'GET' sur la facade");
		return this.endPointClientFacade.getClientSideReference().get(key);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#put(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		System.out.println("Envoi de la requête 'PUT' sur la facade");
		return this.endPointClientFacade.getClientSideReference().put(key, value);
	}

	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#remove(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'REMOVE' sur la facade");
		return this.endPointClientFacade.getClientSideReference().remove(key);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#mapReduce(fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		System.out.println("Envoi de la requête 'MAP REDUCE' sur la facade");
		return this.endPointClientFacade.getClientSideReference().mapReduce(selector, processor, reductor, combinator,
				initialAcc);
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.AbstractComponent#start()
	 */
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting client component.");
		try {

			this.connectToClockServer();

			if (!endPointClientFacade.clientSideInitialised()) {
				this.endPointClientFacade.initialiseClientSide(this);
			}

			super.start();

		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.AbstractComponent#execute()
	 */
	@Override
	public void execute() throws Exception {
		this.logMessage("executing client component." + isStarted());

		Instant i0 = dhtClock.getStartInstant();
		Instant i1 = i0.plusSeconds(60);

		long delay = dhtClock.nanoDelayUntilInstant(i1);

		this.scheduleTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {

				} catch (Exception e) {
					e.printStackTrace();
				} finally {

				}
			}
		}, delay, TimeUnit.NANOSECONDS);

	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#finalise()
	 */
	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping client component.");
		this.printExecutionLogOnFile("client");
		this.endPointClientFacade.cleanUpClientSide();
		this.doPortDisconnection(p.getPortURI());
		p.unpublishPort();
		p.destroyPort();
		super.finalise();
	}

}
