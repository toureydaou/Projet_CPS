package etape3.connecteurs;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

/**
 * La classe <code>MapReduceResultReceptionConnector</code> est un connecteur
 * utilisé pour transférer les résultats d'une opération MapReduce d'un nœud
 * émetteur vers un nœud récepteur.
 * 
 * <p>
 * Cette classe relaie l'appel à {@code acceptResult} vers le service distant
 * désigné par {@code offering}.
 * </p>
 * 
 * <p>
 * Elle hérite de {@link AbstractConnector} fourni par BCM pour la connexion
 * standard.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class MapReduceResultReceptionConnector extends AbstractConnector implements MapReduceResultReceptionCI {

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI#acceptResult(java.lang.String,
	 *      java.lang.String, java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		((MapReduceResultReceptionCI) this.offering).acceptResult(computationURI, emitterId, acc);
	}

}
