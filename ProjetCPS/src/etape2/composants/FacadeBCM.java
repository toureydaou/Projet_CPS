

package etape2.composants;

import java.io.Serializable;

import etape2.endpoints.CompositeMapContentSyncEndpoint;
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
 * spécifiques à l'endpoint {@link CompositeMapContentSyncEndpoint}.
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
public class FacadeBCM extends AbstractComponent implements DHTServicesI{

	// URI constants pour l'accès aux services
	private static final String GET_URI = "GET";
	private static final String PUT_URI = "PUT";
	private static final String REMOVE_URI = "REMOVE";
	private static final String MAPREDUCE_URI = "MAPREDUCE";

	private static final int SCHEDULABLE_THREADS = 1;
	private static final int THREADS_NUMBER = 0;

	// Endpoints pour accéder aux services
	protected CompositeMapContentSyncEndpoint cmce;
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
	protected FacadeBCM(String uri, CompositeMapContentSyncEndpoint cmce, DHTServicesEndPoint dsep)
			throws ConnectionException {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.cmce = cmce;
		this.dsep = dsep;
		dsep.initialiseServerSide(this);
	}

	
	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#get(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {

		String request_uri = URIGenerator.generateURI(GET_URI);
		System.out.println("Reception de la requête 'GET' sur la facade, identifiant de la requete : " + request_uri);
		ContentDataI result = this.cmce.getContentAccessEndpoint().getClientSideReference().getSync(request_uri, key);
		this.cmce.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
		System.out.println(
				"Renvoi de la réponse de la requête 'GET' au client,  identifiant de la requete : " + request_uri);
		return result;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#put(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI)
	 */
	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String request_uri = URIGenerator.generateURI(PUT_URI);
		System.out.println("Reception de la requête 'PUT' sur la facade identifiant requete : " + request_uri);
		ContentDataI result = this.cmce.getContentAccessEndpoint().getClientSideReference().putSync(request_uri, key,
				value);
		this.cmce.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
		System.out.println(
				"Renvoi de la réponse de la requête 'PUT' au client,  identifiant de la requete : " + request_uri);
		return result;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#remove(fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI)
	 */
	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(REMOVE_URI);
		System.out.println("Reception de la requête 'REMOVE' sur la facade identifiant requete : " + request_uri);
		ContentDataI result = this.cmce.getContentAccessEndpoint().getClientSideReference().removeSync(request_uri,
				key);
		this.cmce.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
		System.out.println(
				"Renvoi de la réponse de la requête 'REMOVE' au client,  identifiant de la requete : " + request_uri);
		return result;
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI#mapReduce(fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI, fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI, A)
	 */
	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {

		String uriTete = URIGenerator.generateURI(MAPREDUCE_URI);
		System.out.println("Reception de la requête 'MAP REDUCE' sur la facade identifiant requete : " + uriTete);
		this.cmce.getMapReduceEndpoint().getClientSideReference().mapSync(uriTete, selector, processor);
		A result = this.cmce.getMapReduceEndpoint().getClientSideReference().reduceSync(uriTete, reductor, combinator,
				initialAcc);
		this.cmce.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(uriTete);
		System.out.println(
				"Renvoi de la réponse de la requête 'MAP REDUCE' au client,  identifiant de la requete : " + uriTete);
		return result;
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#start()
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
	 * @see fr.sorbonne_u.components.AbstractComponent#finalise()
	 */
	@Override
	public synchronized void finalise() throws Exception {
		this.logMessage("stopping facade component.");
		this.printExecutionLogOnFile("facade");
		this.cmce.cleanUpClientSide();
		super.finalise();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdown()
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
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdownNow()
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
