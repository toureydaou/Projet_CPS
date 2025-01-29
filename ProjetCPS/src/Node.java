import java.io.Serializable;
import java.util.HashMap;
import java.util.stream.Stream;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class Node implements ContentAccessSyncI,MapReduceSyncI{
	HashMap<ContentKeyI, ContentDataI> content;
	IntInterval intervalle;
	Node suivant;
	String uri;
	Boolean enTete = false;
	HashMap<String, Stream<ContentDataI>> memory = new HashMap<>();
	
	public Node(IntInterval intervalle) {
		super();
		this.content = new HashMap<ContentKeyI, ContentDataI>();
		this.intervalle = intervalle;
		this.suivant = null;
		enTete = false;
	}

	public void setSuivant(Node suivant) {
		this.suivant = suivant;
	}

	@Override
	public <R extends Serializable> void mapSync(String computationURI, SelectorI selector, ProcessorI<R> processor)
			throws Exception {
		memory.put(computationURI, (Stream<ContentDataI>) content.values().stream()
				.filter(selector)
				.map(processor));
		if (this.suivant.enTete == true) return;
		this.suivant.mapSync(computationURI, selector, processor);
	}

	@Override
	public <A extends Serializable, R> A reduceSync(String computationURI, ReductorI<A, R> reductor,
			CombinatorI<A> combinator, A currentAcc) throws Exception {
		
		if (this.suivant.enTete == true) {
			return memory.get(computationURI)
					.reduce(currentAcc,(u,d)-> reductor.apply(u,(R) d), combinator);
		}
		
		return combinator.apply(memory.get(computationURI)
				.reduce(currentAcc,(u,d)-> reductor.apply(u,(R) d), combinator),
				this.suivant.reduceSync(computationURI, reductor, combinator, currentAcc));
	}

	@Override
	public void clearMapReduceComputation(String computationURI) throws Exception {
		memory.remove(computationURI);
		if(this.suivant.enTete == true) return;
		suivant.clearMapReduceComputation(computationURI);
	}

	@Override
	public ContentDataI getSync(String computationURI, ContentKeyI key) throws Exception {
		int n = ((EntierKey) key).getCle();		
		if(intervalle.in(n)) {
			return content.get(key);
		}
		if(this.suivant.enTete == true) return null;
		return this.suivant.getSync(computationURI, key);
	}

	@Override
	public ContentDataI putSync(String computationURI, ContentKeyI key, ContentDataI value) throws Exception {
		int n = ((EntierKey) key).getCle();	
		if(intervalle.in(n)) {
			ContentDataI valuePrec = content.get(key);
			this.content.put(key, value);
			return valuePrec;
		}
		if(this.suivant.enTete == true) return null;
		return this.suivant.putSync(computationURI, key, value);
	}

	@Override
	public ContentDataI removeSync(String computationURI, ContentKeyI key) throws Exception {
		int n = ((EntierKey) key).getCle();	
		if(intervalle.in(n)) {
			ContentDataI valuePrec = content.get(key);
			this.content.remove(key);
			return valuePrec;
		}
		if(this.suivant.enTete == true) return null;
		return this.suivant.removeSync(computationURI, key);
	}

	@Override
	public void clearComputation(String computationURI) throws Exception {
		// TODO Auto-generated method stub
		
	}

	
	
	

}