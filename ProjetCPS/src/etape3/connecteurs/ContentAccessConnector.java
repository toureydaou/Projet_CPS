package etape3.connecteurs;

import etape2.connecteurs.ContentAccessSyncConnector;
import fr.sorbonne_u.components.endpoints.EndPointI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

/**
 * La classe <code>ContentAccessConnector</code> implémente un connecteur 
 * pour les opérations d'accès aux contenus dans une DHT (Distributed Hash Table) 
 * dans un cadre de calcul distribué MapReduce.
 * 
 * <p>
 * Ce connecteur permet d'invoquer de manière asynchrone et distante les méthodes
 * offertes par un service de type {@link ContentAccessCI}. Il encapsule les
 * appels `get`, `put`, `remove`, et `clearComputation`, en les déléguant
 * au composant serveur référencé dans l'attribut `offering`.
 * </p>
 * 
 * <p>
 * Hérite de {@link ContentAccessSyncConnector} pour intégrer des comportements
 * synchrones de base.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class ContentAccessConnector extends ContentAccessSyncConnector implements ContentAccessCI {

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI#get(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void get(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		((ContentAccessCI) this.offering).get(computationURI, key, caller);

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI#put(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void put(String computationURI, ContentKeyI key, ContentDataI value,
			EndPointI<I> caller) throws Exception {
		((ContentAccessCI) this.offering).put(computationURI, key, value, caller);

	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI#remove(java.lang.String,
	 *      fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI,
	 *      fr.sorbonne_u.components.endpoints.EndPointI)
	 */
	@Override
	public <I extends ResultReceptionCI> void remove(String computationURI, ContentKeyI key, EndPointI<I> caller)
			throws Exception {
		((ContentAccessCI) this.offering).remove(computationURI, key, caller);
	}

	/**
	 * 
	 * @see etape2.connecteurs.ContentAccessSyncConnector#clearComputation(java.lang.String)
	 */
	@Override
	public void clearComputation(String computationURI) throws Exception {
		((ContentAccessCI) this.offering).clearComputation(computationURI);
	}

}
