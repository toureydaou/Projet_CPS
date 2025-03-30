package etape3.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class AsynchronousCompositeMapContentEndPoint extends BCMCompositeEndPoint implements ContentNodeBaseCompositeEndPointI<ContentAccessCI, MapReduceCI>{
	private static final long serialVersionUID = 1L;
	/** Nombre de points d'accès dans ce point d'accès composite */
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
		this.mapReduceEndpoint= new AsynchronousMapReduceEndPoint();
		this.addEndPoint(mapReduceEndpoint);
	}
	
	public void setExecutorServiceIndexContentAccessService(int executorServiceIndexContentAccessService) {
		this.contentAccessEndPoint.setExecutorIndex(executorServiceIndexContentAccessService);
	}

	public void setExecutorServiceIndexMapReduceService(int executorServiceIndexMapReduceService) {
		this.mapReduceEndpoint.setExecutorIndex(executorServiceIndexMapReduceService);
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
	public EndPointI<MapReduceCI> getMapReduceEndpoint() {
		return this.getEndPoint(MapReduceCI.class);
	}
}
