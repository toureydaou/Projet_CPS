/**
 * @author	Awwal Fagbehouro
 * @author  Touré-Ydaou TEOURI
 */
package etape2.composants;

import java.io.Serializable;

import etape2.endpoints.CompositeMapContentEndpoint;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

/**
 * 
 */
@OfferedInterfaces(offered = {DHTServicesCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class})
public class FacadeBCM extends AbstractComponent implements DHTServicesI {

	protected DHTServicesEndPoint dsep;
	protected CompositeMapContentEndpoint cmce;
	
	protected FacadeBCM(String uri, DHTServicesEndPoint dsep, CompositeMapContentEndpoint cmce) {
		super(uri, 0, 1);
		this.dsep = dsep;
		this.cmce = cmce;
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.cmce.getContentAccessEndPoint().getClientSideReference().getSync(Uri, key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
