package etape2.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

/**
 * CompositeMapContentEndpoint est un point d'accès composite qui regroupe deux
 * points d'accès distincts : un pour l'accès aux contenus et un pour les
 * opérations de MapReduce. Il étend BCMCompositeEndPoint pour gérer plusieurs
 * points d'accès.
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */

public class CompositeMapContentSyncEndpoint extends BCMCompositeEndPoint implements ContentNodeBaseCompositeEndPointI<ContentAccessSyncCI, MapReduceSyncCI>{
	private static final long serialVersionUID = 1L;
	/** Nombre de points d'accès dans ce point d'accès composite */
	protected static final int NUMBER_OF_ENDPOINTS = 2;

	/**
	 * Constructeur qui initialise les points d'accès nécessaires. Il ajoute deux
	 * points d'accès : un pour l'accès aux contenus et un pour MapReduce.
	 */
	public CompositeMapContentSyncEndpoint() {
		super(NUMBER_OF_ENDPOINTS);
		ContentAccessSyncEndPoint contentAccessEndPoint = new ContentAccessSyncEndPoint();
		this.addEndPoint(contentAccessEndPoint);
		MapReduceSyncEndPoint mapReduceEndpoint = new MapReduceSyncEndPoint();
		this.addEndPoint(mapReduceEndpoint);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getContentAccessEndpoint()
	 */
	@Override
	public EndPointI<ContentAccessSyncCI> getContentAccessEndpoint() {
		return (ContentAccessSyncEndPoint) this.getEndPoint(ContentAccessSyncCI.class);
	}

	/**
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.endpoints.ContentNodeBaseCompositeEndPointI#getMapReduceEndpoint()
	 */
	@Override
	public EndPointI<MapReduceSyncCI> getMapReduceEndpoint() {
		return (MapReduceSyncEndPoint) this.getEndPoint(MapReduceSyncCI.class);
	}
	
}
