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
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
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
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

/**
 * 
 */
@OfferedInterfaces(offered = {DHTServicesCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class})
public class FacadeBCM extends AbstractComponent implements DHTServicesI {

	private static final String GET_URL = "GET";
	private static final String PUT_URL = "PUT";
	private static final String REMOVE_URL = "REMOVE";
	private static final String MAPREDUCE_URL = "MAPREDUCE";
	
	protected CompositeMapContentEndpoint cmce;
	protected DHTServicesEndPoint dsep;
	 
	
	protected FacadeBCM(String uri, CompositeMapContentEndpoint cmce, DHTServicesEndPoint dsep) throws ConnectionException {
		super(uri, 0, 1);
		this.cmce = cmce;
		this.dsep = dsep;
		dsep.initialiseServerSide(this);
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.cmce.getContentAccessEndPoint().getClientSideReference().getSync(URIGenerator.generateURI(GET_URL), key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return this.cmce.getContentAccessEndPoint().getClientSideReference().putSync(URIGenerator.generateURI(PUT_URL), key, value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.cmce.getContentAccessEndPoint().getClientSideReference().removeSync(URIGenerator.generateURI(PUT_URL), key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
	
		String uriTete = URIGenerator.generateURI(MAPREDUCE_URL);
		this.cmce.getMapReduceEndPoint().getClientSideReference().mapSync(uriTete, selector, processor);
		return this.cmce.getMapReduceEndPoint().getClientSideReference().reduceSync(uriTete, reductor, combinator, initialAcc);
	}
	
	@Override
	public void		start() throws ComponentStartException
	{
		this.logMessage("starting facade component.") ;
		super.start() ;
		
		try {
			if (!this.cmce.clientSideInitialised()) {
				this.cmce.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e) ;
		}
	}
	
	@Override
	public void			finalise() throws Exception
	{
		this.logMessage("stopping facade component.");
		this.printExecutionLogOnFile("facade");
		this.cmce.cleanUpClientSide();
		super.finalise();
	}
	
	@Override
	public void			shutdown() throws ComponentShutdownException
	{
		try {
			this.dsep.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}
	
	@Override
	public void			shutdownNow() throws ComponentShutdownException
	{
		try {
			this.dsep.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}
	

	
	
	

}
