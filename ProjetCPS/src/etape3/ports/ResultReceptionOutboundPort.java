package etape3.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class ResultReceptionOutboundPort extends AbstractOutboundPort implements ResultReceptionCI{
	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port sortant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public  ResultReceptionOutboundPort (ComponentI owner) throws Exception {
		super(ResultReceptionCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade tous deux jouant le role
		// de client
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ResultReceptionOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ResultReceptionCI.class, owner);

		assert uri != null && owner != null;
	}

	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		((ResultReceptionCI) this.getConnector()).acceptResult(computationURI, result);

	}
}
