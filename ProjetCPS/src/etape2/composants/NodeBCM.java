package etape2.composants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import etape1.EntierKey;
import etape2.endpoints.CompositeMapContentEndpoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

@OfferedInterfaces(offered = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
@RequiredInterfaces(required = { ContentAccessSyncCI.class, MapReduceSyncCI.class })
public class NodeBCM extends AbstractComponent implements ContentAccessSyncI, MapReduceSyncI {

	private HashMap<ContentKeyI, ContentDataI> content;
	private IntInterval intervalle;
	private ArrayList<String> uriPassCont = new ArrayList<>();
	private ArrayList<String> uriPassMap = new ArrayList<>();
	private HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();

	protected CompositeMapContentEndpoint cmceInbound; // endpoint coté serveur
	protected CompositeMapContentEndpoint cmceOutbound; // endpoint coté client

	protected NodeBCM(String uri, CompositeMapContentEndpoint cmceInbound, CompositeMapContentEndpoint cmceOutbound,
			IntInterval intervalle) throws ConnectionException {
		super(uri, 0, 2);
		this.content = new HashMap<>();
		this.intervalle = intervalle;
		this.cmceInbound = cmceInbound;
		this.cmceOutbound = cmceOutbound;
		cmceInbound.initialiseServerSide(this);
	}

	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		if (!uriPassMap.contains(computationURI)) {
			uriPassMap.add(computationURI);
			memory.put(computationURI,
					(Stream<ContentDataI>) content.values().stream().filter(selector).map(processor));
			this.cmceOutbound.getMapReduceEndPoint().getClientSideReference().mapSync(computationURI, selector,
					processor);
		} else {
			return;
		}

	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {

		if (uriPassMap.contains(computationURI)) {
			uriPassMap.remove(computationURI);
			return combinator.apply(
					memory.get(computationURI).reduce(currentAcc, (u, d) -> reductor.apply(u, (R) d), combinator),
					this.cmceOutbound.getMapReduceEndPoint().getClientSideReference().reduceSync(computationURI,
							reductor, combinator, currentAcc));
		} else {

			return currentAcc;
		}

	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		if (memory.containsKey(computationURI)) {
			memory.remove(computationURI);
			this.cmceOutbound.getMapReduceEndPoint().getClientSideReference().clearMapReduceComputation(computationURI);
		}

	}

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int n = ((EntierKey) key).getCle();
			if (intervalle.in(n)) {

				return this.get(key);
			}

			return this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().getSync(computationURI, key);
		}
	}

	private ContentDataI get(ContentKeyI key) {
		for (ContentKeyI k : content.keySet()) {

			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {

				return content.get(k);
			}
		}
		return null;
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int n = ((EntierKey) key).getCle();
			if (intervalle.in(n)) {

				ContentDataI valuePrec = this.get(key);
				this.put(key, value);
				return valuePrec;
			}
			return this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().putSync(computationURI, key,
					value);

		}
	}

	private void put(ContentKeyI key, ContentDataI data) {
		for (ContentKeyI k : content.keySet()) {
			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {
				content.put(k, data);
				return;
			}
		}
		content.put(key, data);
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		if (uriPassCont.contains(computationURI)) {
			return null;
		} else {
			uriPassCont.add(computationURI);
			int n = ((EntierKey) key).getCle();
			if (intervalle.in(n)) {
				ContentDataI valuePrec = this.get(key);
				this.remove(key);
				return valuePrec;
			}
			return this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().removeSync(computationURI,
					key);
		}

	}

	private void remove(ContentKeyI key) {

		for (ContentKeyI k : content.keySet()) {
			if (((EntierKey) k).getCle() == ((EntierKey) key).getCle()) {
				content.remove(k);
			}
		}

	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		if (uriPassCont.contains(computationURI)) {
			uriPassCont.remove(computationURI);
			this.cmceOutbound.getContentAccessEndPoint().getClientSideReference().clearComputation(computationURI);
		}
	}

	@Override
	public synchronized void start() throws ComponentStartException {
		this.logMessage("starting node component.");
		super.start();
		try {
			if (!this.cmceOutbound.clientSideInitialised()) {
				this.cmceOutbound.initialiseClientSide(this);
			}
		} catch (ConnectionException e) {
			throw new ComponentStartException(e);
		}
	}

	@Override
	public synchronized void finalise() throws Exception {
		this.logMessage("stopping node component.");
		this.printExecutionLogOnFile("node");
		this.cmceOutbound.cleanUpClientSide();
		super.finalise();
	}

	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		try {
			this.cmceInbound.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdownNow()
	 */
	@Override
	public synchronized void shutdownNow() throws ComponentShutdownException {
		try {
			this.cmceInbound.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

}
