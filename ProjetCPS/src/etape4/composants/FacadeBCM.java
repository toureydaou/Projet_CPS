package etape4.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import etape2.endpoints.DHTServicesEndPoint;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape3.endpoints.ResultReceptionEndPoint;
import etape4.endpoints.CompositeMapContentManagementEndPoint;
import etape4.policies.IgnoreChordsPolicy;
import etape4.policies.LoadPolicy;
import etape4.policies.ThreadsPolicy;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * La classe <code>FacadeBCM</code> représente un composant qui envoie 
 * à la DHT des requêtes d'accès au contenu et des opérations
 * MapReduce de manière asynchrone. Elle implémente les interfaces
 * <code>ResultReceptionI</code>, <code>MapReduceResultReceptionI</code>
 * afin de recevoir les résultats des requêtes. En plus de cela la facade
 * lance sur les noeuds les opérations SPLIT et MERGE à des moments précis
 * de son cycle de vie
 * 
 * @see ResultReceptionI
 * @see MapReduceResultReceptionI
 * @see DHTServicesI
 * @see AbstractComponent
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@OfferedInterfaces(offered = { DHTServicesCI.class, ResultReceptionCI.class, MapReduceResultReceptionCI.class })
@RequiredInterfaces(required = { ContentAccessCI.class, ParallelMapReduceCI.class, DHTManagementCI.class })
public class FacadeBCM extends AbstractComponent implements ResultReceptionI, MapReduceResultReceptionI, DHTServicesI {

	// URI constants pour l'accès aux services
	private static final String GET_URI_PREFIX = "GET";
	private static final String PUT_URI_PREFIX = "PUT";
	private static final String REMOVE_URI_PREFIX = "REMOVE";
	private static final String MAPREDUCE_URI_PREFIX = "MAPREDUCE";
	private static final String COMPUTE_CHORDS_URI_PREFIX = "COMPUTE-CHORDS";
	private static final String SPLIT_URI_PREFIX = "SPLIT";
	private static final String MERGE_URI_PREFIX = "MERGE";

	private static final int NUMBER_OF_CHORDS = 4;

	private static final int SCHEDULABLE_THREADS = 2;
	private static final int THREADS_NUMBER = 2;

	private static final int START_POLICY = 0;

	// Limite à partir de laquelle on peut considérer un split ou un merge dans
	// toute la table
	private static final int LIMIT_NUMBER_WRITE_OPERATIONS = 10;

	private boolean LIMIT_REACHED = false;

	// Nombre d'opérations PUT et REMOVE sur la table
	private AtomicInteger number_write_operation = new AtomicInteger(0);

	protected CompositeMapContentManagementEndPoint endPointFacadeNoeud;
	protected DHTServicesEndPoint endPointClientFacade;
	protected ResultReceptionEndPoint resultatReceptionEndPoint;
	protected MapReduceResultReceptionEndPoint mapReduceResultatReceptionEndPoint;

	private HashMap<String, CompletableFuture<Serializable>> resultsContentAccess;
	private HashMap<String, CompletableFuture<Serializable>> resultsMapReduce;

	// Synchronisation pour éviter une exécution parallèle des opérations
	// split/merge avec d'autres opérations
	protected final ReentrantReadWriteLock splitMergeLock;

	/**
	 * Instancie la facade
	 * 
	 * @param uri
	 * @param endPointFacadeNoeud
	 * @param endPointClientFacade
	 * @throws ConnectionException
	 */
	protected FacadeBCM(String uri, CompositeMapContentManagementEndPoint endPointFacadeNoeud,
			DHTServicesEndPoint endPointClientFacade) throws ConnectionException {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.endPointFacadeNoeud = endPointFacadeNoeud;
		this.endPointClientFacade = endPointClientFacade;
		this.resultatReceptionEndPoint = new ResultReceptionEndPoint();
		this.mapReduceResultatReceptionEndPoint = new MapReduceResultReceptionEndPoint();
		this.resultsContentAccess = new HashMap<String, CompletableFuture<Serializable>>();
		this.resultsMapReduce = new HashMap<String, CompletableFuture<Serializable>>();
		this.splitMergeLock = new ReentrantReadWriteLock();

		this.endPointClientFacade.initialiseServerSide(this);
		this.resultatReceptionEndPoint.initialiseServerSide(this);
		this.mapReduceResultatReceptionEndPoint.initialiseServerSide(this);

		this.createNewExecutorService(ThreadsPolicy.RESULT_RECEPTION_HANDLER_URI,
				ThreadsPolicy.NUMBER_ACCEPT_RESULT_CONTENT_ACCESS_THREADS, true);

		this.createNewExecutorService(ThreadsPolicy.MAP_REDUCE_RESULT_RECEPTION_HANDLER_URI,
				ThreadsPolicy.NUMBER_ACCEPT_RESULT_MAP_REDUCE_THREADS, true);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#get(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		this.splitMergeLock.readLock().lock();
		ContentDataI value;
		String request_uri = URIGenerator.generateURI(GET_URI_PREFIX);
		try {
			System.out
					.println("Reception de la requête 'GET' sur la facade, identifiant de la requete : " + request_uri);
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			this.resultsContentAccess.put(request_uri, f);
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().get(request_uri, key,
					resultatReceptionEndPoint);
			value = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);

		} finally {
			this.resultsContentAccess.remove(request_uri);
			this.splitMergeLock.readLock().unlock();
		}
		return value;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#put(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {

		this.splitMergeLock.readLock().lock();
		ContentDataI oldValue;
		String request_uri = URIGenerator.generateURI(PUT_URI_PREFIX + "-" + key.hashCode());
		try {
			this.countNumberOfOperations();
			System.out.println("Reception de la requête 'PUT' sur la facade identifiant requete : " + request_uri);
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			this.resultsContentAccess.put(request_uri, f);
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().put(request_uri, key, value,
					resultatReceptionEndPoint);
			oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);

		} finally {
			this.resultsContentAccess.remove(request_uri);
			this.splitMergeLock.readLock().unlock();
		}

		return oldValue;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#remove(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {

		this.splitMergeLock.readLock().lock();
		ContentDataI oldValue;
		String request_uri = URIGenerator.generateURI(REMOVE_URI_PREFIX);
		try {
			this.countNumberOfOperations();

			System.out.println("Reception de la requête 'REMOVE' sur la facade identifiant requete : " + request_uri);
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			this.resultsContentAccess.put(request_uri, f);
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().remove(request_uri, key,
					resultatReceptionEndPoint);
			oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);

		} finally {
			this.resultsContentAccess.remove(request_uri);
			this.splitMergeLock.readLock().unlock();
		}
		return oldValue;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#mapReduce(fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {

		this.splitMergeLock.readLock().lock();
		A result;
		String request_uri = URIGenerator.generateURI(MAPREDUCE_URI_PREFIX);
		try {

			System.out
					.println("Reception de la requête 'MAP REDUCE' sur la facade identifiant requete : " + request_uri);
			CompletableFuture<Serializable> reduceResult = new CompletableFuture<Serializable>();
			resultsMapReduce.put(request_uri, reduceResult);

			this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().parallelMap(request_uri, selector,
					processor, new IgnoreChordsPolicy(START_POLICY));
			this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().parallelReduce(request_uri,
					reductor, combinator, initialAcc, initialAcc, new IgnoreChordsPolicy(START_POLICY),
					this.mapReduceResultatReceptionEndPoint);
			result = (A) resultsMapReduce.get(request_uri).get();

			this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference()
					.clearMapReduceComputation(request_uri);

		} finally {
			this.resultsMapReduce.remove(request_uri);
			this.splitMergeLock.readLock().unlock();
		}
		return result;
	}

	public void clearComputation(String computationURI) throws Exception {
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
	}

	public void clearMapReduceComputation(String computationURI) throws Exception {
		this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference()
				.clearMapReduceComputation(computationURI);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI#acceptResult(java.lang.String,
	 *      java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {

		this.resultsContentAccess.get(computationURI).complete(result);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI#acceptResult(java.lang.String,
	 *      java.lang.String, java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) {

		this.resultsMapReduce.get(computationURI).complete(acc);
	}

	/**
	 * Méthode qui lance un split sur tous les noeuds de l'anneau DHT
	 * 
	 * @throws Exception
	 */
	public void split() throws Exception {
		this.splitMergeLock.writeLock().lock();
		String split_uri = URIGenerator.generateURI(SPLIT_URI_PREFIX);
		try {
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();

			this.resultsContentAccess.put(split_uri, f);
			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference().split(split_uri,
					new LoadPolicy(), this.resultatReceptionEndPoint);

			this.resultsContentAccess.get(split_uri).get();
			System.out.println("Fin split");

			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference()
					.computeChords(URIGenerator.generateURI(COMPUTE_CHORDS_URI_PREFIX), NUMBER_OF_CHORDS);
		} finally {
			this.resultsContentAccess.remove(split_uri);
			this.splitMergeLock.writeLock().unlock();
		}

	}

	/**
	 * Méthode qui lance un merge sur tous les noeuds de l'anneau DHT
	 * 
	 * @throws Exception
	 */
	public void merge() throws Exception {
		this.splitMergeLock.writeLock().lock();
		String merge_uri = URIGenerator.generateURI(MERGE_URI_PREFIX);
		try {
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();

			this.resultsContentAccess.put(merge_uri, f);
			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference().merge(merge_uri,
					new LoadPolicy(), this.resultatReceptionEndPoint);
			this.resultsContentAccess.get(merge_uri).get();

			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference()
					.computeChords(URIGenerator.generateURI(COMPUTE_CHORDS_URI_PREFIX), NUMBER_OF_CHORDS);
		} finally {
			this.resultsContentAccess.remove(merge_uri);
			this.splitMergeLock.writeLock().unlock();
		}

	}

	/**
	 * Méthode qui compte le nombre d'opérations PUT et REMOVE que la facade a
	 * transmis à la DHT
	 */
	public void countNumberOfOperations() {
		int count = number_write_operation.incrementAndGet();
		if ((count != 0) && (count % LIMIT_NUMBER_WRITE_OPERATIONS) == 0) {
			LIMIT_REACHED = true;
		}
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#start()
	 */
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting facade component.");
		super.start();
		try {
			if (!this.endPointFacadeNoeud.clientSideInitialised()) {
				this.endPointFacadeNoeud.initialiseClientSide(this);
			}

		} catch (Exception e) {
			throw new ComponentStartException(e);
		}
	}

	/**
	 * La facade lancera chaque seconde une vérification sur le nombre de requêtes
	 * traitées et celles en cours afin de savaoir si elle peut lancer des
	 * opérations split et merge sur la DHT.
	 * 
	 * @see fr.sorbonne_u.components.AbstractComponent#execute()
	 */
	@Override
	public void execute() throws Exception {
		super.execute();

		this.runTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {
					((FacadeBCM) this.taskOwner).endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference()
							.computeChords(COMPUTE_CHORDS_URI_PREFIX, NUMBER_OF_CHORDS);

					while (true) {
						Thread.sleep(1000); // pause de 1 seconde

						if (LIMIT_REACHED) {
							System.out.println(">> Déclenchement Split/Merge après " + LIMIT_NUMBER_WRITE_OPERATIONS
									+ " opérations");
							((FacadeBCM) this.getTaskOwner()).split();
							((FacadeBCM) this.getTaskOwner()).merge();
							LIMIT_REACHED = false; // seulement après split/merge
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#finalise()
	 */
	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping facade component.");
		this.printExecutionLogOnFile("facade");
		this.endPointFacadeNoeud.cleanUpClientSide();
		super.finalise();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdown()
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			System.out.println("client endpoint shutdown");
			this.endPointClientFacade.cleanUpServerSide();

			System.out.println("resultat endpoint shutdown");
			this.resultatReceptionEndPoint.cleanUpServerSide();

			System.out.println("map resultat endpoint shutdown");
			this.mapReduceResultatReceptionEndPoint.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdownNow()
	 */
	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.endPointClientFacade.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

}
