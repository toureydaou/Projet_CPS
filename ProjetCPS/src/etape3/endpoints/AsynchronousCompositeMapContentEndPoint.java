package etape3.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;

public class AsynchronousCompositeMapContentEndPoint extends BCMCompositeEndPoint {
	private static final long serialVersionUID = 1L;
	/** Nombre de points d'accès dans ce point d'accès composite */
	protected static final int NUMBER_OF_ENDPOINTS = 2;

	/**
	 * Constructeur qui initialise les points d'accès nécessaires. Il ajoute deux
	 * points d'accès : un pour l'accès aux contenus et un pour MapReduce.
	 */
	public AsynchronousCompositeMapContentEndPoint() {
		super(NUMBER_OF_ENDPOINTS);
		AsynchronousContentAccessEndPoint contentAccessEndPoint = new AsynchronousContentAccessEndPoint();
		this.addEndPoint(contentAccessEndPoint);
		AsynchronousMapReduceEndPoint mapReduceEndpoint = new AsynchronousMapReduceEndPoint();
		this.addEndPoint(mapReduceEndpoint);
	}

	/**
	 * Accède au point d'accès dédié à l'accès aux contenus.
	 * 
	 * @return Le point d'accès pour l'accès aux contenus.
	 */
	public AsynchronousContentAccessEndPoint getContentAccessEndPoint() {
		return (AsynchronousContentAccessEndPoint) this.getEndPoint(ContentAccessCI.class);
	}

	/**
	 * Accède au point d'accès dédié aux opérations MapReduce.
	 * 
	 * @return Le point d'accès pour les opérations MapReduce.
	 */
	public AsynchronousMapReduceEndPoint getMapReduceEndPoint() {
		return (AsynchronousMapReduceEndPoint) this.getEndPoint(MapReduceCI.class);
	}
}

