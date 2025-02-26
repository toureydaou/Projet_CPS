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
@OfferedInterfaces(offered = { DHTServicesCI.class })
@RequiredInterfaces(required = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class FacadeBCM extends AbstractComponent implements DHTServicesI {

	private static final String GET_URI = "GET";
	private static final String PUT_URI = "PUT";
	private static final String REMOVE_URI = "REMOVE";
	private static final String MAPREDUCE_URI = "MAPREDUCE";

	protected CompositeMapContentEndpoint cmce;
	protected DHTServicesEndPoint dsep;

	protected FacadeBCM(String uri, CompositeMapContentEndpoint cmce, DHTServicesEndPoint dsep)
			throws ConnectionException {
		super(uri, 0, 1);
		this.cmce = cmce;
		this.dsep = dsep;
		dsep.initialiseServerSide(this);
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(GET_URI);
		ContentDataI result = this.cmce.getContentAccessEndPoint().getClientSideReference().getSync(request_uri, key);
		this.cmce.getContentAccessEndPoint().getClientSideReference().clearComputation(request_uri);
		return result;
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		String request_uri = URIGenerator.generateURI(PUT_URI);
		ContentDataI result = this.cmce.getContentAccessEndPoint().getClientSideReference().putSync(request_uri, key,
				value);
		this.cmce.getContentAccessEndPoint().getClientSideReference().clearComputation(request_uri);
		return result;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		String request_uri = URIGenerator.generateURI(REMOVE_URI);
		ContentDataI result = this.cmce.getContentAccessEndPoint().getClientSideReference().removeSync(request_uri,
				key);
		this.cmce.getContentAccessEndPoint().getClientSideReference().clearComputation(request_uri);
		return result;
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {

		String uriTete = URIGenerator.generateURI(MAPREDUCE_URI);
		this.cmce.getMapReduceEndPoint().getClientSideReference().mapSync(uriTete, selector, processor);
		A result = this.cmce.getMapReduceEndPoint().getClientSideReference().reduceSync(uriTete, reductor, combinator,
				initialAcc);
		this.cmce.getMapReduceEndPoint().getClientSideReference().clearMapReduceComputation(uriTete);
		return result;
	}

	@Override
	public synchronized void start() throws ComponentStartException {
		this.logMessage("starting facade component.");
		super.start();

		try {
			if (!this.cmce.clientSideInitialised()) {
				this.cmce.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	@Override
	public synchronized void finalise() throws Exception {
		this.logMessage("stopping facade component.");
		this.printExecutionLogOnFile("facade");
		this.cmce.cleanUpClientSide();
		super.finalise();
	}

	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		try {
			this.dsep.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	@Override
	public synchronized void shutdownNow() throws ComponentShutdownException {
		try {
			this.dsep.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

}
