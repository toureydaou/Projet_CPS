package etape3.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;


/**
 * The Class AsynchronousCompositeMapContentEndPoint.
 */
public class AsynchronousCompositeMapContentEndPoint extends BCMCompositeEndPoint
		implements ContentNodeBaseCompositeEndPointI<ContentAccessCI, MapReduceCI> {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** Nombre de points d'accès dans ce point d'accès composite. */
	protected static final int NUMBER_OF_ENDPOINTS = 2;

	/** The content access end point. */
	private AsynchronousContentAccessEndPoint contentAccessEndPoint;

	/** The map reduce endpoint. */
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
	 * Sets the executor service index content access service.
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
	 * Définit l'index du service d'exécution pour le service d'accès au contenu.
	 *
	 * @param executorServiceIndexContentAccessService L'index du service
	 *                                                 d'exécution pour l'accès au
	 *                                                 contenu à définir.
	 */
	@Override
	public EndPointI<ContentAccessCI> getContentAccessEndpoint() {
		return this.getEndPoint(ContentAccessCI.class);
	}

	/**
	 * Récupère l'endpoint de MapReduce.
	 *
	 * @return l'endpoint MapReduce.
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getMapReduceEndpoint()
	 */
	@Override
	public EndPointI<MapReduceCI> getMapReduceEndpoint() {
		return this.getEndPoint(MapReduceCI.class);
	}
}
