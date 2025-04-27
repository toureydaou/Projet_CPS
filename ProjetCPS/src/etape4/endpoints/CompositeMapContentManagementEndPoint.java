package etape4.endpoints;

import etape3.endpoints.AsynchronousContentAccessEndPoint;
import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;

/**
 * La classe {@code CompositeMapContentManagementEndPoint} est un point d'accès composite
 * pour un nœud de contenu dans une DHT utilisant MapReduce.
 * <p>
 * Elle regroupe trois services : l'accès aux contenus, MapReduce, et la gestion de la DHT,
 * chacun représenté par un point d'accès distinct.
 * </p>
 * 
 * @see BCMCompositeEndPoint
 * @see ContentNodeCompositeEndPointI
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO 
 */
public class CompositeMapContentManagementEndPoint extends BCMCompositeEndPoint implements ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>{
	private static final long serialVersionUID = 1L;
	
	protected static final int NUMBER_OF_ENDPOINTS = 3;

	private AsynchronousContentAccessEndPoint contentAccessEndPoint;
	private ParallelMapReduceEndPoint mapReduceEndpoint;
	private DHTManagementEndPoint dhtManagementEndPoint;

	/**
	 * Construit un nouveau {@code CompositeMapContentManagementEndPoint}.
	 * <p>
	 * Ce constructeur initialise et ajoute les trois points d'accès :
	 * accès au contenu, MapReduce, et gestion de la DHT.
	 * </p>
	 */
	public CompositeMapContentManagementEndPoint() {
		super(NUMBER_OF_ENDPOINTS);
		this.contentAccessEndPoint = new AsynchronousContentAccessEndPoint();
		this.addEndPoint(contentAccessEndPoint);
		this.mapReduceEndpoint= new ParallelMapReduceEndPoint();
		this.addEndPoint(mapReduceEndpoint);
		this.dhtManagementEndPoint= new DHTManagementEndPoint();
		this.addEndPoint(dhtManagementEndPoint);
	}
	
	/**
	 * Associe un exécuteur spécifique au service d'accès au contenu.
	 *
	 * @param executorServiceIndexContentAccessService l'index de l'exécuteur à utiliser.
	 */
	public void setExecutorServiceIndexContentAccessService(int executorServiceIndexContentAccessService) {
		this.contentAccessEndPoint.setExecutorIndex(executorServiceIndexContentAccessService);
	}
	
	/**
	 * Associe un exécuteur spécifique au service d'accès au service map/reduce.
	 *
	 * @param executorServiceIndexContentAccessService l'index de l'exécuteur à utiliser.
	 */
	public void setExecutorServiceIndexMapReduceService(int executorServiceIndexMapReduceService) {
		this.mapReduceEndpoint.setExecutorIndex(executorServiceIndexMapReduceService);
	}
	
	/**
	 * Associe un exécuteur spécifique au service d'accès au service de gestion de la DHT.
	 *
	 * @param executorServiceIndexContentAccessService l'index de l'exécuteur à utiliser.
	 */
	public void setExecutorServiceIndexDHTManagementService(int executorServiceIndexContentAccessService) {
		this.dhtManagementEndPoint.setExecutorIndex(executorServiceIndexContentAccessService);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getContentAccessEndpoint()
	 */
	@Override
	public EndPointI<ContentAccessCI> getContentAccessEndpoint() {
		return this.getEndPoint(ContentAccessCI.class);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getMapReduceEndpoint()
	 */
	@Override
	public EndPointI<ParallelMapReduceCI> getMapReduceEndpoint() {
		return this.getEndPoint(ParallelMapReduceCI.class);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI#getDHTManagementEndpoint()
	 */
	@Override
	public EndPointI<DHTManagementCI> getDHTManagementEndpoint() {
		return this.getEndPoint(DHTManagementCI.class);
	}
}
