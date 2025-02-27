package etape2.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
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

public class CompositeMapContentEndpoint extends BCMCompositeEndPoint {
	private static final long serialVersionUID = 1L;
	/** Nombre de points d'accès dans ce point d'accès composite */
	protected static final int NUMBER_OF_ENDPOINTS = 2;

	/**
	 * Constructeur qui initialise les points d'accès nécessaires. Il ajoute deux
	 * points d'accès : un pour l'accès aux contenus et un pour MapReduce.
	 */
	public CompositeMapContentEndpoint() {
		super(NUMBER_OF_ENDPOINTS);
		ContentAccessEndPoint contentAccessEndPoint = new ContentAccessEndPoint();
		this.addEndPoint(contentAccessEndPoint);
		MapReduceEndPoint mapReduceEndpoint = new MapReduceEndPoint();
		this.addEndPoint(mapReduceEndpoint);
	}

	/**
	 * Accède au point d'accès dédié à l'accès aux contenus.
	 * 
	 * @return Le point d'accès pour l'accès aux contenus.
	 */
	public ContentAccessEndPoint getContentAccessEndPoint() {
		return (ContentAccessEndPoint) this.getEndPoint(ContentAccessSyncCI.class);
	}

	/**
	 * Accède au point d'accès dédié aux opérations MapReduce.
	 * 
	 * @return Le point d'accès pour les opérations MapReduce.
	 */
	public MapReduceEndPoint getMapReduceEndPoint() {
		return (MapReduceEndPoint) this.getEndPoint(MapReduceSyncCI.class);
	}
}
