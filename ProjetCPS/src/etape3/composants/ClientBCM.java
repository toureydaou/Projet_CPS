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
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.utils.aclocks.AcceleratedClock;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerConnector;
import fr.sorbonne_u.utils.aclocks.ClocksServerOutboundPort;

// TODO: Auto-generated Javadoc
/**
 * ClientBCM est un composant client qui interagit avec le service DHT pour
 * effectuer des opérations de type MapReduce, ainsi que des opérations CRUD sur
 * les données stockées dans le DHT.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

@RequiredInterfaces(required = { DHTServicesCI.class, ClocksServerCI.class })
public class ClientBCM extends AbstractComponent {

	/** The end point client facade. */
	protected DHTServicesEndPoint endPointClientFacade; // Point d'accès aux services DHT

	/** The dht clock. */
	protected AcceleratedClock dhtClock; // Référence à l'horloge

	/** The Constant SCHEDULABLE_THREADS. */
	private static final int SCHEDULABLE_THREADS = 1;

	/** The Constant THREADS_NUMBER. */
	private static final int THREADS_NUMBER = 0;

	/** The p. */
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
	 * Connect to clock server.
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
	 * Récupère une donnée à partir de la clé fournie.
	 * 
	 * @param key Clé de la donnée à récupérer.
	 * @return La donnée correspondante à la clé.
	 * @throws Exception Si une erreur se produit lors de la récupération.
	 */
	public ContentDataI get(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'GET' sur la facade");
		return this.endPointClientFacade.getClientSideReference().get(key);
	}

	/**
	 * Ajoute ou met à jour une donnée associée à une clé dans le DHT.
	 * 
	 * @param key   La clé associée à la donnée.
	 * @param value La donnée à stocker.
	 * @return La donnée précédente associée à la clé (ou null si c'est un nouvel
	 *         élément).
	 * @throws Exception Si une erreur se produit lors de l'ajout.
	 */
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		System.out.println("Envoi de la requête 'PUT' sur la facade");
		return this.endPointClientFacade.getClientSideReference().put(key, value);
	}

	/**
	 * Supprime une donnée associée à une clé dans le DHT.
	 * 
	 * @param key La clé associée à la donnée à supprimer.
	 * @return La donnée supprimée.
	 * @throws Exception Si une erreur se produit lors de la suppression.
	 */
	public ContentDataI remove(ContentKeyI key) throws Exception {
		System.out.println("Envoi de la requête 'REMOVE' sur la facade");
		return this.endPointClientFacade.getClientSideReference().remove(key);
	}

	/**
	 * Effectue une opération MapReduce sur les données stockées dans le DHT.
	 *
	 * @param <R>        the generic type
	 * @param <A>        the generic type
	 * @param selector   Un sélecteur pour filtrer les données.
	 * @param processor  Un processeur pour transformer les données.
	 * @param reductor   Un réducteur pour agréger les résultats.
	 * @param combinator Un combinator pour combiner les résultats intermédiaires.
	 * @param initialAcc L'accumulateur initial.
	 * @return Le résultat final de l'opération MapReduce.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */
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
	 * 
	 *
	 * @throws Exception the exception
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
