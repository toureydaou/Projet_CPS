package etape4.endpoints;

import etape3.endpoints.AsynchronousContentAccessEndPoint;
import etape3.endpoints.AsynchronousMapReduceEndPoint;
import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;

public class CompositeMapContentManagementEndPoint extends BCMCompositeEndPoint implements ContentNodeCompositeEndPointI<ContentAccessCI, ParallelMapReduceCI, DHTManagementCI>{
	private static final long serialVersionUID = 1L;
	/** Nombre de points d'accès dans ce point d'accès composite */
	protected static final int NUMBER_OF_ENDPOINTS = 3;

	private AsynchronousContentAccessEndPoint contentAccessEndPoint;
	private ParallelMapReduceEndPoint mapReduceEndpoint;
	private DHTManagementEndPoint dhtManagementEndPoint;

	/**
	 * Constructeur qui initialise les points d'accès nécessaires. Il ajoute deux
	 * points d'accès : un pour l'accès aux contenus et un pour MapReduce.
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
	public EndPointI<ParallelMapReduceCI> getMapReduceEndpoint() {
		return this.getEndPoint(ParallelMapReduceCI.class);
	}

	@Override
	public EndPointI<DHTManagementCI> getDHTManagementEndpoint() {

		return this.getEndPoint(DHTManagementCI.class);
	}
}
