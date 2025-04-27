package etape3.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

/**
 * La classe <code>AsynchronousCompositeMapContentEndPoint</code> représente un
 * point d'accès composite pour un nœud de contenu dans un système de type DHT
 * (Distributed Hash Table) intégrant des services d'accès aux contenus et de
 * MapReduce.
 *
 * <p>
 * Cette classe regroupe deux points d'accès indépendants :
 * <ul>
 * <li>Un point d'accès pour accéder aux contenus (ContentAccessCI).</li>
 * <li>Un point d'accès pour soumettre et exécuter des tâches MapReduce
 * (MapReduceCI).</li>
 * </ul>
 * Elle permet ainsi à un composant de gérer les deux types de service via une
 * seule structure composite.
 * </p>
 *
 * <p>
 * Chaque point d'accès peut être associé à un service d'exécution distinct pour
 * optimiser la gestion concurrente des requêtes.
 * </p>
 *
 * @author Touré-Ydaou TEOURI
 * 
 * @author Awwal FAGBEHOURO
 */
public class AsynchronousCompositeMapContentEndPoint extends BCMCompositeEndPoint
		implements ContentNodeBaseCompositeEndPointI<ContentAccessCI, MapReduceCI> {

	private static final long serialVersionUID = 1L;

	/** Nombre de points d'accès dans ce point d'accès composite. */
	protected static final int NUMBER_OF_ENDPOINTS = 2;

	private AsynchronousContentAccessEndPoint contentAccessEndPoint;

	private AsynchronousMapReduceEndPoint mapReduceEndpoint;

	/**
	 * Constructeur qui initialise les points d'accès nécessaires. Il ajoute deux
	 * points d'accès : un pour l'accès aux contenus et un pour MapReduce.
	 */
	public AsynchronousCompositeMapContentEndPoint() {
		super(NUMBER_OF_ENDPOINTS);
		this.contentAccessEndPoint = new AsynchronousContentAccessEndPoint();
		this.addEndPoint(contentAccessEndPoint);
		this.mapReduceEndpoint = new AsynchronousMapReduceEndPoint();
		this.addEndPoint(mapReduceEndpoint);
	}

	/**
	 * Définit l'index du service d'exécution pour le service ContentAcesss
	 *
	 * @param executorServiceIndexContentAccessService the new executor service
	 *                                                 index content access service
	 */
	public void setExecutorServiceIndexContentAccessService(int executorServiceIndexContentAccessService) {
		this.contentAccessEndPoint.setExecutorIndex(executorServiceIndexContentAccessService);
	}

	/**
	 * Définit l'index du service d'exécution pour le service MapReduce.
	 *
	 * @param executorServiceIndexMapReduceService L'index du service d'exécution
	 *                                             MapReduce à définir.
	 */
	public void setExecutorServiceIndexMapReduceService(int executorServiceIndexMapReduceService) {
		this.mapReduceEndpoint.setExecutorIndex(executorServiceIndexMapReduceService);
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getContentAccessEndpoint()
	 */
	@Override
	public EndPointI<ContentAccessCI> getContentAccessEndpoint() {
		return this.getEndPoint(ContentAccessCI.class);
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getMapReduceEndpoint()
	 */
	@Override
	public EndPointI<MapReduceCI> getMapReduceEndpoint() {
		return this.getEndPoint(MapReduceCI.class);
	}
}
