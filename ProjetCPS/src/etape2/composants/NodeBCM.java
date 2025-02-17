package etape2.composants;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
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
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

@OfferedInterfaces(offered = {ContentAccessSyncCI.class, MapReduceSyncCI.class})
@RequiredInterfaces(required = {ContentAccessSyncCI.class, MapReduceSyncCI.class})
public class NodeBCM extends AbstractComponent implements ContentAccessSyncI, MapReduceSyncI {
	
	protected NodeBCM(int nbThreads, int nbSchedulableThreads) {
		super(nbThreads, nbSchedulableThreads);
		// TODO Auto-generated constructor stub
	}
	private HashMap<ContentKeyI, ContentDataI> content;
	private IntInterval intervalle;
	private ArrayList<String> uriPassCont = new ArrayList<>();
	private ArrayList<String> uriPassMap = new ArrayList<>();
	private HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();
	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void clearComputation(String computationURI) throws Exception {
		// TODO Auto-generated method stub
		
	}	

}
