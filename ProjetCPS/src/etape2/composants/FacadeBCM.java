
package etape2.composants;

import java.io.Serializable;

import etape2.endpoints.CompositeMapContentEndpoint;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * FacadeBCM est un composant qui fait office de façade pour l'accès aux
 * services DHT (Distributed Hash Table) et aux opérations de MapReduce via
 * l'interface {@link DHTServicesI}. Il délègue les appels aux services
 * spécifiques à l'endpoint {@link CompositeMapContentEndpoint}.
 * 
 * 
 * <p>
 * <strong>Description</strong>
 * </p>
 * <p>
 * Ce composant offre plusieurs opérations synchrones : - Récupération, ajout et
 * suppression de données via des clés DHT, - Exécution de calculs MapReduce sur
 * les données DHT.
 * 
 * Les interfaces offertes sont {@link DHTServicesCI}, et il nécessite les
 * interfaces {@link ContentAccessSyncCI} et {@link MapReduceSyncCI}.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

@OfferedInterfaces(offered = { DHTServicesCI.class })
@RequiredInterfaces(required = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class FacadeBCM extends AbstractComponent implements DHTServicesI {

	// URI constants pour l'accès aux services
	private static final String GET_URI = "GET";
	private static final String PUT_URI = "PUT";
	private static final String REMOVE_URI = "REMOVE";
	private static final String MAPREDUCE_URI = "MAPREDUCE";

	// Endpoints pour accéder aux services
	protected CompositeMapContentEndpoint cmce;
	protected DHTServicesEndPoint dsep;

	/**
	 * Constructeur pour initialiser le composant FacadeBCM.
	 * 
	 * @param uri  L'URI du composant FacadeBCM.
	 * @param cmce L'endpoint CompositeMapContentEndpoint utilisé pour accéder aux
	 *             services DHT.
	 * @param dsep L'endpoint DHTServicesEndPoint pour la gestion des services DHT.
	 * @throws ConnectionException Si une erreur de connexion se produit.
	 */
	protected FacadeBCM(String uri, CompositeMapContentEndpoint cmce, DHTServicesEndPoint dsep)
			throws ConnectionException {
		super(uri, 0, 1);
		this.cmce = cmce;
		this.dsep = dsep;
		dsep.initialiseServerSide(this);
	}

	/**
	 * Récupère les données associées à une clé via le service DHT.
	 * 
	 * @param key La clé des données à récupérer.
	 * @return Les données associées à la clé.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(GET_URI);
		ContentDataI result = this.cmce.getContentAccessEndPoint().getClientSideReference().getSync(request_uri, key);
		this.cmce.getContentAccessEndPoint().getClientSideReference().clearComputation(request_uri);
		return result;
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
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String request_uri = URIGenerator.generateURI(PUT_URI);
		ContentDataI result = this.cmce.getContentAccessEndPoint().getClientSideReference().putSync(request_uri, key,
				value);
		this.cmce.getContentAccessEndPoint().getClientSideReference().clearComputation(request_uri);
		return result;
	}

	/**
	 * Supprime les données associées à une clé dans le service DHT.
	 * 
	 * @param key La clé des données à supprimer.
	 * @return Les données supprimées, ou null si aucune donnée n'existait.
	 * @throws Exception Si une erreur se produit lors de l'exécution.
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(REMOVE_URI);
		ContentDataI result = this.cmce.getContentAccessEndPoint().getClientSideReference().removeSync(request_uri,
				key);
		this.cmce.getContentAccessEndPoint().getClientSideReference().clearComputation(request_uri);
		return result;
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
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {

		String uriTete = URIGenerator.generateURI(MAPREDUCE_URI);
		this.cmce.getMapReduceEndPoint().getClientSideReference().mapSync(uriTete, selector, processor);
		A result = this.cmce.getMapReduceEndPoint().getClientSideReference().reduceSync(uriTete, reductor, combinator,
				initialAcc);
		this.cmce.getMapReduceEndPoint().getClientSideReference().clearMapReduceComputation(uriTete);
		return result;
	}

	/**
	 * Démarre le composant FacadeBCM.
	 * 
	 * @throws ComponentStartException Si une erreur se produit lors du démarrage du
	 *                                 composant.
	 */
	@Override
	public synchronized void start() throws ComponentStartException {
		this.logMessage("starting facade component.");
		super.start();

		try {
			if (!this.cmce.clientSideInitialised()) {
				this.cmce.initialiseClientSide(this);
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
	public synchronized void finalise() throws Exception {
		this.logMessage("stopping facade component.");
		this.printExecutionLogOnFile("facade");
		this.cmce.cleanUpClientSide();
		super.finalise();
	}

	/**
	 * Effectue un arrêt propre du composant FacadeBCM.
	 * 
	 * @throws ComponentShutdownException Si une erreur se produit lors de l'arrêt.
	 */
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		try {
			this.dsep.cleanUpServerSide();
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
	public synchronized void shutdownNow() throws ComponentShutdownException {
		try {
			this.dsep.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

}
