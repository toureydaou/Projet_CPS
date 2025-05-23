package etape3.composants;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import etape2.composants.NodeBCM;
import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
import etape4.policies.ThreadsPolicy;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

/**
 * La classe <code>AsynchronousNodeBCM</code> représente un composant de nœud
 * dans un système distribué qui gère l'accès au contenu et les opérations
 * MapReduce de manière asynchrone. Elle implémente les interfaces
 * <code>ContentAccessI</code> et <code>MapReduceI</code>. Ce composant utilise
 * la communication asynchrone et gère les données dans une table de hachage
 * distribuée (DHT).
 * 
 * Elle gère trois types principaux d'opérations :
 * <ul>
 * <li>Accès au contenu (GET, PUT, REMOVE) pour stocker et récupérer des données
 * depuis la DHT.</li>
 * <li>Opérations MapReduce (MAP, REDUCE) pour traiter des données en utilisant
 * le paradigme MapReduce.</li>
 * <li>Nettoyage des calculs pour effacer les données précédemment
 * stockées.</li>
 * </ul>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
@OfferedInterfaces(offered = { MapReduceCI.class, ContentAccessCI.class })
@RequiredInterfaces(required = { MapReduceCI.class, ContentAccessCI.class, ResultReceptionCI.class,
		MapReduceResultReceptionCI.class })
public class AsynchronousNodeBCM extends NodeBCM implements ContentAccessI, MapReduceI {

	private static final int SCHEDULABLE_THREADS = 0;

	private static final int THREADS_NUMBER = 1;

	protected ConcurrentHashMap<Integer, ContentDataI> content;

	protected IntInterval intervalle;

	// Listes des URI des computations MapReduce et de stockage déjà traitées
	protected CopyOnWriteArrayList<String> listeUriContentOperations = new CopyOnWriteArrayList<>();

	protected CopyOnWriteArrayList<String> listeUriMapOperations = new CopyOnWriteArrayList<>();

	protected CopyOnWriteArrayList<String> listeUriReduceOperations = new CopyOnWriteArrayList<>();

	// Mémoire temporaire pour stocker les résultats intermédiaires des computations
	// MapReduce
	protected ConcurrentHashMap<String, CompletableFuture<Stream<ContentDataI>>> memory = new ConcurrentHashMap<>();

	protected AsynchronousCompositeMapContentEndPoint compositeMapContentEndpointOutboundAsync;

	protected AsynchronousCompositeMapContentEndPoint compositeMapContentEndpointInboundAsync;

	protected final ReentrantReadWriteLock hashMapLock;

	protected String uri;

	/**
	 * Crée un asynchronous node BCM.
	 *
	 * @param uri                               the uri
	 * @param compositeMapEndpointInboundAsync  the composite map endpoint inbound
	 *                                          async
	 * @param compositeMapEndpointOutboundAsync the composite map endpoint outbound
	 *                                          async
	 * @param intervalle                        the intervalle
	 * @throws ConnectionException the connection exception
	 */
	protected AsynchronousNodeBCM(String uri, AsynchronousCompositeMapContentEndPoint compositeMapEndpointInboundAsync,
			AsynchronousCompositeMapContentEndPoint compositeMapEndpointOutboundAsync, IntInterval intervalle)
			throws ConnectionException {
		super(THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.content = new ConcurrentHashMap<>();
		this.intervalle = intervalle;
		this.compositeMapContentEndpointInboundAsync = compositeMapEndpointInboundAsync;
		this.compositeMapContentEndpointOutboundAsync = compositeMapEndpointOutboundAsync;
		this.hashMapLock = new ReentrantReadWriteLock();

		this.compositeMapContentEndpointInboundAsync.initialiseServerSide(this);
		
		this.createNewExecutorService(ThreadsPolicy.CONTENT_ACCESS_HANDLER_URI,
				ThreadsPolicy.NUMBER_CONTENT_ACCESS_THREADS, true);
		this.createNewExecutorService(ThreadsPolicy.MAP_REDUCE_HANDLER_URI,
				ThreadsPolicy.NUMBER_MAP_REDUCE_THREADS, true);

	}

	/**
	 * Crée un node BCM.
	 *
	 * @param uri        the uri
	 * @param intervalle the intervalle
	 * @throws ConnectionException the connection exception
	 */
	protected AsynchronousNodeBCM(String uri, IntInterval intervalle) throws ConnectionException {
		super(THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.uri = uri;
		this.content = new ConcurrentHashMap<>();
		this.intervalle = intervalle;
		this.hashMapLock = new ReentrantReadWriteLock();
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI#get(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'GET' sur le noeud " + this.intervalle.first());
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {

				this.hashMapLock.readLock().lock();
				try {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					caller.getClientSideReference().acceptResult(computationURI, content.get(key.hashCode()));
					caller.cleanUpClientSide();
				} finally {
					this.hashMapLock.readLock().unlock();
				}
			} else {
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
						.get(computationURI, key, caller);
			}

		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'GET' sur la facade depuis le noeud " + this.intervalle.first());
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI#put(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		System.out.println("Reception de la requete 'PUT' le noeud " + this.intervalle.first()
				+ " identifiant requete : " + computationURI);

		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				this.hashMapLock.writeLock().lock();
				try {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					ContentDataI oldValue = content.put(key.hashCode(), value);
					caller.getClientSideReference().acceptResult(computationURI, oldValue);
					caller.cleanUpClientSide();
				} finally {
					this.hashMapLock.writeLock().unlock();
				}
			} else {
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
						.put(computationURI, key, value, caller);
			}
		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out.println("Envoi du résultat du 'PUT' sur la facade depuis le noeud " + this.intervalle.first());
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessI#remove(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		System.out.println("Reception de la requete 'REMOVE' le noeud " + this.intervalle.first()
				+ " identifiant requete : " + computationURI);
		if (!listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.addIfAbsent(computationURI);

			if (this.intervalle.in(key.hashCode())) {
				this.hashMapLock.writeLock().lock();
				try {
					if (!caller.clientSideInitialised()) {
						caller.initialiseClientSide(this);
					}
					ContentDataI oldValue = content.remove(key.hashCode());
					caller.getClientSideReference().acceptResult(computationURI, oldValue);
					caller.cleanUpClientSide();
				} finally {
					this.hashMapLock.writeLock().unlock();
				}
			} else {
				this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
						.remove(computationURI, key, caller);
			}
		} else {
			if (!caller.clientSideInitialised()) {
				caller.initialiseClientSide(this);
			}
			System.out
					.println("Envoi du résultat du 'REMOVE' sur la facade depuis le noeud " + this.intervalle.first());
			// la valeur de hachage de la clé se situe en dehors de l'intervalle de clés de
			// la DHT
			caller.getClientSideReference().acceptResult(computationURI, null);
			caller.cleanUpClientSide();
		}
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI#map(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <R extends Serializable> void map(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		System.out.println("Reception de la requete 'MAP REDUCE' (MAP) sur le noeud " + this.intervalle.first()
				+ " identifiant requete : " + computationURI);
		if (!listeUriMapOperations.contains(computationURI)) {
			listeUriMapOperations.addIfAbsent(computationURI);
			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndpoint().getClientSideReference()
					.map(computationURI, selector, processor);
			this.hashMapLock.readLock().lock();
			try {

				CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
				memory.putIfAbsent(computationURI, futureStream);
				memory.get(computationURI)
						.complete((Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));

			} finally {
				this.hashMapLock.readLock().unlock();
			}

		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceI#reduce(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A, A,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <A extends Serializable, R, I extends MapReduceResultReceptionCI> void reduce(String computationURI,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A identityAcc, A currentAcc, EndPointI<I> callerNode)
			throws Exception {
		System.out.println("Reception de la requete 'MAP REDUCE' (REDUCE) sur le noeud " + this.intervalle.first()
				+ " identifiant requete : " + computationURI);
		if (!listeUriReduceOperations.contains(computationURI)) {
			listeUriReduceOperations.add(computationURI);

			CompletableFuture<Stream<ContentDataI>> futureStream = new CompletableFuture<Stream<ContentDataI>>();
			memory.putIfAbsent(computationURI, futureStream);

			Stream<ContentDataI> localStream = memory.get(computationURI).get();

			A localReduce = localStream.reduce(identityAcc, (u, d) -> reductor.apply(u, (R) d), combinator);
			localReduce = combinator.apply(currentAcc, localReduce);

			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndpoint().getClientSideReference()
					.reduce(computationURI, reductor, combinator, identityAcc, localReduce, callerNode);
		} else {
			if (!callerNode.clientSideInitialised()) {
				callerNode.initialiseClientSide(this);
			}
			System.out.println(
					"Envoi du résultat du 'MAP REDUCE' sur la facade depuis le noeud " + this.intervalle.first());
			callerNode.getClientSideReference().acceptResult(computationURI, "", currentAcc);
			callerNode.cleanUpClientSide();
		}

	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI#clearComputation(java.lang.String)
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (listeUriContentOperations.contains(computationURI)) {
			listeUriContentOperations.remove(computationURI);
		} else {
			this.compositeMapContentEndpointOutboundAsync.getContentAccessEndpoint().getClientSideReference()
					.clearComputation(computationURI);
		}
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI#clearMapReduceComputation(java.lang.String)
	 */
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (listeUriMapOperations.contains(computationURI) && listeUriReduceOperations.contains(computationURI)) {
			this.listeUriMapOperations.remove(computationURI);
			this.listeUriReduceOperations.remove(computationURI);
			this.memory.remove(computationURI);
		} else {
			this.compositeMapContentEndpointOutboundAsync.getMapReduceEndpoint().getClientSideReference()
					.clearMapReduceComputation(computationURI);
		}
	}

	/**
	 * @see etape2.composants.NodeBCM#start()
	 */
	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.startOrigin();
		try {
			if (!this.compositeMapContentEndpointOutboundAsync.clientSideInitialised()) {
				this.compositeMapContentEndpointOutboundAsync.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	/**
	 * Start origin.
	 *
	 * @throws ComponentStartException the component start exception
	 */
	public void startOrigin() throws ComponentStartException {
		super.startOrigin();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#finalise()
	 */
	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping node component.");
		this.compositeMapContentEndpointOutboundAsync.cleanUpClientSide();
		super.finaliseOrigin();
	}

	/**
	 * Finalise origin.
	 *
	 * @throws Exception the exception
	 */
	public void finaliseOrigin() throws Exception {
		super.finaliseOrigin();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdown()
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.compositeMapContentEndpointInboundAsync.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownOrigin();
	}

	/**
	 * Shutdown origin.
	 *
	 * @throws ComponentShutdownException the component shutdown exception
	 */
	public void shutdownOrigin() throws ComponentShutdownException {
		super.shutdownOrigin();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdownNow()
	 */
	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.compositeMapContentEndpointInboundAsync.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNowOrigin();
	}

	/**
	 * Shutdown now origin.
	 *
	 * @throws ComponentShutdownException the component shutdown exception
	 */
	public void shutdownNowOrigin() throws ComponentShutdownException {
		super.shutdownNowOrigin();
	}

}