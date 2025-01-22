import java.io.Serializable;
import java.util.HashMap;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;

public class Node implements ContentAccessSyncI,MapReduceSyncI{
	HashMap<ContentKeyI, ContentDataI> content;
	IntInterval intervalle;
	Node suivant;
	String uri;
	
	public Node(IntInterval intervalle) {
		super();
		this.content = new HashMap<ContentKeyI, ContentDataI>();
		this.intervalle = intervalle;
		this.suivant = null;
		this.uri = URIGenerator.generateURI("node");
	}

	public String getUri() {
		return uri;
	}

	public void setSuivant(Node suivant) {
		this.suivant = suivant;
	}
	
	public Node getSuivant() {
		return suivant;
	}

	public HashMap<ContentKeyI, ContentDataI> getContent() {
		return content;
	}

	public IntInterval getIntervalle() {
		return intervalle;
	}

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
