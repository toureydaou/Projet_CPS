package etape3.connecteurs;

import java.io.Serializable;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

/**
 * La classe <code>ResultReceptionConnector</code> est un connecteur utilisé
 * pour transmettre un résultat intermédiaire ou final d'une opération vers le
 * composant qui a initié la computation.
 * 
 * <p>
 * Cette classe relaie l'appel à {@code acceptResult} vers l'interface distante
 * {@code ResultReceptionCI} désignée par {@code offering}.
 * </p>
 * 
 * <p>
 * Elle hérite de {@link AbstractConnector}, la classe standard de connecteurs
 * dans le framework BCM.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class ResultReceptionConnector extends AbstractConnector implements ResultReceptionCI {

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI#acceptResult(java.lang.String,
	 *      java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		((ResultReceptionCI) this.offering).acceptResult(computationURI, result);
	}

}
