package etape2.endpoints;

import fr.sorbonne_u.components.endpoints.BCMCompositeEndPoint;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;

public class CompositeMapContentEndpoint extends BCMCompositeEndPoint {
	private static final long serialVersionUID = 1L;
	/**	number of end points in this composite end point.					*/
	protected static final int	NUMBER_OF_ENDPOINTS = 2;
	
	public CompositeMapContentEndpoint() {
		super(NUMBER_OF_ENDPOINTS);
		ContentAccessEndPoint contentAccessEndPoint = new ContentAccessEndPoint();
		this.addEndPoint(contentAccessEndPoint);
		MapReduceEndPoint mapReduceEndpoint = new MapReduceEndPoint();
		this.addEndPoint(mapReduceEndpoint);
	}
	
	public ContentAccessEndPoint getContentAccessEndPoint() {
		return (ContentAccessEndPoint) this.getEndPoint(ContentAccessSyncCI.class);
	}
	
	public MapReduceEndPoint getMapReduceEndPoint() {
		return (MapReduceEndPoint) this.getEndPoint(MapReduceSyncCI.class);
	}
}
