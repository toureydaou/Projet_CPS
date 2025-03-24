package etape3.ports;

import java.io.Serializable;

import etape3.composants.FacadeBCM;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;

public class MapReduceResultReceptionInboundPort extends AbstractInboundPort implements MapReduceResultReceptionCI {

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
	public MapReduceResultReceptionInboundPort(ComponentI owner) throws Exception {
		super(MapReduceResultReceptionCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof MapReduceResultReceptionI);
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param owner Composant propriétaire du port.
	 * @throws Exception <i>to do</i>.
	 */
	public MapReduceResultReceptionInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, MapReduceResultReceptionCI.class, owner);

		assert uri != null && (owner instanceof MapReduceResultReceptionI);
	}


	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		this.getOwner().runTask(owner -> {
			try {
				((FacadeBCM) owner).acceptResult(computationURI, emitterId, acc);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

}
