import java.io.Serializable;

import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class Facade implements DHTServicesI {
	Node tete;

	public Facade(int nbNoeuds) {
		tete = new Node(new IntInterval(0, 99));
		Node prec =tete;
		Node suiv = null;
		for(int i = 1; i < nbNoeuds; i++) {
			suiv = new Node(new IntInterval(i*100, (i*100) + 99));
			prec.setSuivant(suiv);
			prec = suiv;
			suiv = suiv.getSuivant();
		}
		prec.setSuivant(tete);
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		int n = ((EntierKey) key).getCle();
		int i=1;
		if(tete.getIntervalle().in(n)) {
			return tete.getContent().get(key);
		}
		Node courant = tete.getSuivant();
		while(courant!=tete) {
			i++;
			if(courant.getIntervalle().in(n)) {
				System.out.println(i);
				return courant.getContent().get(key);
			}
			courant = courant.getSuivant();
		}
		return null;
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		// TODO Auto-generated method stub
		int n = ((EntierKey) key).getCle();
		ContentDataI valuePrec = null;
		if(tete.getIntervalle().in(n)) {
			valuePrec= tete.getContent().get(key);
			tete.getContent().put(key, value);
			return valuePrec;
		}
		Node courant = tete.getSuivant();
		while(courant!=tete) {
			if(courant.getIntervalle().in(n)) {
				valuePrec= courant.getContent().get(key);
				courant.getContent().put(key, value);
				return valuePrec;
			}
			courant = courant.getSuivant();
		}
		return null;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		// TODO Auto-generated method stub
		int n = ((EntierKey) key).getCle();
		ContentDataI valuePrec = null;
		if(tete.getIntervalle().in(n)) {
			valuePrec= tete.getContent().get(key);
			tete.getContent().remove(key);
			return valuePrec;
		}
		Node courant = tete.getSuivant();
		while(courant!=tete) {
			if(courant.getIntervalle().in(n)) {
				valuePrec= courant.getContent().get(key);
				courant.getContent().remove(key);
				return valuePrec;
			}
			courant = courant.getSuivant();
		}
		return null;
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
