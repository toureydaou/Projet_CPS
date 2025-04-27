package etape3.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import etape2.endpoints.DHTServicesEndPoint;
import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape3.endpoints.ResultReceptionEndPoint;
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
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * La classe <code>FacadeBCM</code> représente un composant qui envoie 
 * à la DHT des requêtes d'accès au contenu et des opérations
 * MapReduce de manière asynchrone. Elle implémente les interfaces
 * <code>ResultReceptionI</code>, <code>MapReduceResultReceptionI</code>
 * afin de recevoir les résultats des requêtes
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
@RequiredInterfaces(required = { ContentAccessCI.class, MapReduceCI.class })
public class FacadeBCM extends AbstractComponent implements ResultReceptionI, MapReduceResultReceptionI, DHTServicesI {

	// URI constants pour l'accès aux services
	private static final String GET_URI_PREFIX = "GET";

	private static final String PUT_URI_PREFIX = "PUT";

	private static final String REMOVE_URI_PREFIX = "REMOVE";

	private static final String MAPREDUCE_URI_PREFIX = "MAPREDUCE";

	private static final int SCHEDULABLE_THREADS = 0;

	private static final int THREADS_NUMBER = 1;

	// Endpoints pour accéder aux services
	protected AsynchronousCompositeMapContentEndPoint endPointFacadeNoeud;

	protected DHTServicesEndPoint endPointClientFacade;

	protected ResultReceptionEndPoint resultatReceptionEndPoint;

	protected MapReduceResultReceptionEndPoint mapReduceResultatReceptionEndPoint;

	private HashMap<String, CompletableFuture<Serializable>> resultsContentAccess;

	private HashMap<String, CompletableFuture<Serializable>> resultsMapReduce;

	/**
	 * Constructeur pour initialiser le composant FacadeBCM.
	 * 
	 * @param uri                  L'URI du composant FacadeBCM.
	 * @param endPointFacadeNoeud  L'endpoint CompositeMapContentEndpoint utilisé
	 *                             pour accéder aux services DHT.
	 * @param endPointClientFacade L'endpoint DHTServicesEndPoint pour la gestion
	 *                             des services DHT.
	 * @throws ConnectionException Si une erreur de connexion se produit.
	 */
	protected FacadeBCM(String uri, AsynchronousCompositeMapContentEndPoint endPointFacadeNoeud,
			DHTServicesEndPoint endPointClientFacade) throws ConnectionException {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.endPointFacadeNoeud = endPointFacadeNoeud;
		this.endPointClientFacade = endPointClientFacade;
		this.resultatReceptionEndPoint = new ResultReceptionEndPoint();
		this.mapReduceResultatReceptionEndPoint = new MapReduceResultReceptionEndPoint();
		this.resultsContentAccess = new HashMap<String, CompletableFuture<Serializable>>();
		this.resultsMapReduce = new HashMap<String, CompletableFuture<Serializable>>();
		
		this.endPointClientFacade.initialiseServerSide(this);
		this.resultatReceptionEndPoint.initialiseServerSide(this);
		this.mapReduceResultatReceptionEndPoint.initialiseServerSide(this);
		
		this.createNewExecutorService(ThreadsPolicy.RESULT_RECEPTION_HANDLER_URI,
				ThreadsPolicy.NUMBER_ACCEPT_RESULT_CONTENT_ACCESS_THREADS, true);
		
		this.createNewExecutorService(ThreadsPolicy.MAP_REDUCE_RESULT_RECEPTION_HANDLER_URI,
				ThreadsPolicy.NUMBER_ACCEPT_RESULT_MAP_REDUCE_THREADS, true);

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#get(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(GET_URI_PREFIX);
		System.out.println("Reception de la requête 'GET' sur la facade, identifiant de la requete : " + request_uri);
		CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
		this.resultsContentAccess.put(request_uri, f);
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().get(request_uri, key,
				resultatReceptionEndPoint);
		ContentDataI value = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
		this.resultsContentAccess.remove(request_uri);
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
		return value;

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#put(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String request_uri = URIGenerator.generateURI(PUT_URI_PREFIX);
		System.out.println("Reception de la requête 'PUT' sur la facade identifiant requete : " + request_uri);
		CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
		this.resultsContentAccess.put(request_uri, f);
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().put(request_uri, key, value,
				resultatReceptionEndPoint);
		ContentDataI oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
		this.resultsContentAccess.remove(request_uri);
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
		return oldValue;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#remove(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(REMOVE_URI_PREFIX);
		System.out.println("Reception de la requête 'REMOVE' sur la facade identifiant requete : " + request_uri);
		CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
		this.resultsContentAccess.put(request_uri, f);
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().remove(request_uri, key,
				resultatReceptionEndPoint);
		ContentDataI oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
		this.resultsContentAccess.remove(request_uri);
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
		return oldValue;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#mapReduce(fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {

		String request_uri = URIGenerator.generateURI(MAPREDUCE_URI_PREFIX);
		System.out.println("Reception de la requête 'MAP REDUCE' sur la facade identifiant requete : " + request_uri);
		CompletableFuture<Serializable> reduceResult = new CompletableFuture<Serializable>();
		resultsMapReduce.put(request_uri, reduceResult);
		this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().map(request_uri, selector, processor);
		this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().reduce(request_uri, reductor,
				combinator, initialAcc, initialAcc, this.mapReduceResultatReceptionEndPoint);
		A result = (A) reduceResult.get();

		this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(request_uri);
		this.resultsMapReduce.remove(request_uri);
		return result;
	}

	/**
	 * Clear computation.
	 *
	 * @param computationURI the computation URI
	 * @throws Exception the exception
	 */
	public void clearComputation(String computationURI) throws Exception {
		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);
	}

	/**
	 * Clear map reduce computation.
	 *
	 * @param computationURI the computation URI
	 * @throws Exception the exception
	 */
	public void clearMapReduceComputation(String computationURI) throws Exception {
		this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference()
				.clearMapReduceComputation(computationURI);

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI#acceptResult(java.lang.String,
	 *      java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		this.resultsContentAccess.get(computationURI).complete(result);
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI#acceptResult(java.lang.String,
	 *      java.lang.String, java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) {
		this.resultsMapReduce.get(computationURI).complete(acc);
	}

	/**
	 * 
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
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	/**
	 * 
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
	 * 
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdown()
	 */
	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			this.endPointClientFacade.cleanUpServerSide();
			this.resultatReceptionEndPoint.cleanUpServerSide();
			this.mapReduceResultatReceptionEndPoint.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	/**
	 * 
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