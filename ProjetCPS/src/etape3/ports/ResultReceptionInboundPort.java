package etape3.ports;

import java.io.Serializable;

import etape3.composants.FacadeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessSyncCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;

public class ResultReceptionInboundPort extends AbstractInboundPort implements ResultReceptionCI {
	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	private static final long serialVersionUID = 1L;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port entrant avec le composant propriétaire.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ResultReceptionInboundPort(ComponentI owner) throws Exception {
		super(ResultReceptionCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof FacadeBCM);
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public ResultReceptionInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ResultReceptionCI.class, owner);

		assert uri != null && (owner instanceof FacadeBCM);
	}

	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		this.getOwner().runTask(owner -> {
			try {
				((FacadeBCM) owner).acceptResult(computationURI, result);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

}
