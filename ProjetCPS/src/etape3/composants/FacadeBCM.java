package etape3.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import etape2.endpoints.DHTServicesEndPoint;
import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape3.endpoints.ResultReceptionEndPoint;
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

@OfferedInterfaces(offered = { DHTServicesCI.class, ResultReceptionCI.class, MapReduceResultReceptionCI.class })
@RequiredInterfaces(required = { ContentAccessCI.class, MapReduceCI.class })
public class FacadeBCM extends AbstractComponent implements ResultReceptionI, MapReduceResultReceptionI, DHTServicesI {

	// URI constants pour l'accès aux services
	private static final String GET_URI = "GET";
	private static final String PUT_URI = "PUT";
	private static final String REMOVE_URI = "REMOVE";
	private static final String MAPREDUCE_URI = "MAPREDUCE";

	private static final int SCHEDULABLE_THREADS = 0;
	private static final int THREADS_NUMBER = 2;

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
	 * @param uri  L'URI du composant FacadeBCM.
	 * @param endPointFacadeNoeud L'endpoint CompositeMapContentEndpoint utilisé pour accéder aux
	 *             services DHT.
	 * @param endPointClientFacade L'endpoint DHTServicesEndPoint pour la gestion des services DHT.
	 * @throws ConnectionException Si une erreur de connexion se produit.
	 */
	protected FacadeBCM(String uri, AsynchronousCompositeMapContentEndPoint endPointFacadeNoeud, DHTServicesEndPoint endPointClientFacade,
			ResultReceptionEndPoint resultatReceptionEndPoint) throws ConnectionException {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.endPointFacadeNoeud = endPointFacadeNoeud;
		this.endPointClientFacade = endPointClientFacade;
		this.resultatReceptionEndPoint = resultatReceptionEndPoint;
		this.resultsContentAccess = new HashMap<String, CompletableFuture<Serializable>>();
		this.resultsMapReduce = new HashMap<String, CompletableFuture<Serializable>>();
		endPointClientFacade.initialiseServerSide(this);
		resultatReceptionEndPoint.initialiseServerSide(this);
	}

	/**
	 * Récupère les données associées à une clé via le service DHT.
	 * 
	 * @param key La clé des données à récupérer.
	 * @return Les données associées à la clé.results.get(request_ur
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */

	public ContentDataI get(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(GET_URI);
		System.out.println("Reception de la requête 'GET' sur la facade, identifiant de la requete : " + request_uri);
		CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
		this.resultsContentAccess.put(request_uri, f);
		this.endPointFacadeNoeud.getContentAccessEndPoint().getClientSideReference().get(request_uri, key, resultatReceptionEndPoint);
		ContentDataI value = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
		this.resultsContentAccess.remove(request_uri);
		return value;

	}

	/**
	 * Ajoute ou met à jour les données associées à une clé dans le service DHT.
	 * 
	 * @param key   La clé des données à ajouter ou mettre à jour.
	 * @param value Les données à ajouter ou mettre à jour.
	 * @return Les données précédemment associées à la clé, ou null si aucune donnée
	 *         n'existait.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String request_uri = URIGenerator.generateURI(PUT_URI);
		System.out.println("Reception de la requête 'PUT' sur la facade identifiant requete : " + request_uri);
		CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
		this.resultsContentAccess.put(request_uri, f);
		this.endPointFacadeNoeud.getContentAccessEndPoint().getClientSideReference().put(request_uri, key, value, resultatReceptionEndPoint);
		ContentDataI oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
		this.resultsContentAccess.remove(request_uri);
		return oldValue;
	}

	/**
	 * Supprime les données associées à une clé dans le service DHT.
	 * 
	 * @param key La clé des données à supprimer.
	 * @return Les données supprimées, ou null si aucune donnée n'existait.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */

	public ContentDataI remove(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(REMOVE_URI);
		System.out.println("Reception de la requête 'REMOVE' sur la facade identifiant requete : " + request_uri);
		CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
		this.resultsContentAccess.put(request_uri, f);
		this.endPointFacadeNoeud.getContentAccessEndPoint().getClientSideReference().remove(request_uri, key, resultatReceptionEndPoint);
		ContentDataI oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
		this.resultsContentAccess.remove(request_uri);
		return oldValue;
	}

	/**
	 * Effectue une opération MapReduce en deux phases (map et reduce) sur les
	 * données du service DHT, puis renvoie le résultat combiné.
	 * 
	 * @param selector   Sélecteur des éléments à traiter.
	 * @param processor  Fonction de traitement pour les éléments.
	 * @param reductor   Fonction de réduction pour combiner les résultats.
	 * @param combinator Fonction pour combiner les résultats finaux.
	 * @param initialAcc Valeur initiale de l'accumulateur pour la réduction.
	 * @param <R>        Type des résultats intermédiaires de la phase map.
	 * @param <A>        Type du résultat final après la phase reduce.
	 * @return Le résultat de l'opération MapReduce.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */

	@SuppressWarnings("unchecked")
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {

		String request_uri = URIGenerator.generateURI(MAPREDUCE_URI);
		System.out.println("Reception de la requête 'MAP REDUCE' sur la facade identifiant requete : " + request_uri);
		CompletableFuture<Serializable> reduceResult = new CompletableFuture<Serializable>();
		resultsMapReduce.put(request_uri, reduceResult);
		this.endPointFacadeNoeud.getMapReduceEndPoint().getClientSideReference().map(request_uri, selector, processor);
		this.endPointFacadeNoeud.getMapReduceEndPoint().getClientSideReference().reduce(request_uri, reductor, combinator, initialAcc, initialAcc, this.mapReduceResultatReceptionEndPoint);
		A result = (A) reduceResult.get();
		this.endPointFacadeNoeud.getMapReduceEndPoint().getClientSideReference().clearMapReduceComputation(request_uri);
		this.resultsMapReduce.remove(request_uri);
		return  result;
	}

	
	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		this.resultsContentAccess.get(computationURI).complete(result);
	}

	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) {
		this.resultsMapReduce.get(computationURI).complete(acc);
	}

	/**
	 * Démarre le composant FacadeBCM.
	 * 
	 * @throws ComponentStartException Si une erreur se produit lors du démarrage du
	 *                                 composant.
	 */
	@Override
	public  void start() throws ComponentStartException {
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
	 * Finalise et arrête le composant FacadeBCM.
	 * 
	 * @throws Exception Si une erreur se produit lors de l'arrêt du composant.
	 */
	@Override
	public  void finalise() throws Exception {
		this.logMessage("stopping facade component.");
		this.printExecutionLogOnFile("facade");
		this.endPointFacadeNoeud.cleanUpClientSide();
		super.finalise();
	}

	/**
	 * Effectue un arrêt propre du composant FacadeBCM.
	 * 
	 * @throws ComponentShutdownException Si une erreur se produit lors de l'arrêt.
	 */
	@Override
	public  void shutdown() throws ComponentShutdownException {
		try {
			this.endPointClientFacade.cleanUpServerSide();
			this.resultatReceptionEndPoint.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	/**
	 * Force un arrêt immédiat du composant FacadeBCM.
	 * 
	 * @throws ComponentShutdownException Si une erreur se produit lors de l'arrêt
	 *                                    immédiat.
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
