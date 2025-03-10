package etape3.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class CompositeMapContentEndPoint extends BCMCompositeEndPoint {
	private static final long serialVersionUID = 1L;
	/** Nombre de points d'accès dans ce point d'accès composite */
	protected static final int NUMBER_OF_ENDPOINTS = 2;

	/**
	 * Constructeur qui initialise les points d'accès nécessaires. Il ajoute deux
	 * points d'accès : un pour l'accès aux contenus et un pour MapReduce.
	 */
	public CompositeMapContentEndPoint() {
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
		return (ContentAccessEndPoint) this.getEndPoint(ContentAccessCI.class);
	}

	/**
	 * Accède au point d'accès dédié aux opérations MapReduce.
	 * 
	 * @return Le point d'accès pour les opérations MapReduce.
	 */
	public MapReduceEndPoint getMapReduceEndPoint() {
		return (MapReduceEndPoint) this.getEndPoint(MapReduceCI.class);
	}
}

