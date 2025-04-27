package etape3.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;

/**
 * La classe <code>MapReduceResultReceptionOutboundPort</code> implémente un
 * port sortant permettant à un composant client d'envoyer des résultats à un
 * composant serveur dans un système MapReduce.
 *
 * <p>
 * Ce port est utilisé par des composants agissant en tant que clients pour
 * recevoir les résultats d'opérations MapReduce. Il permet de transmettre les
 * résultats intermédiaires ou finaux de ces opérations aux composants qui les
 * demandent.
 * </p>
 * 
 * <p>
 * <strong>Propriétaire du port :</strong> Le propriétaire de ce port peut être
 * un noeud ou une façade jouant le rôle de client dans le système.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class MapReduceResultReceptionOutboundPort extends AbstractOutboundPort implements MapReduceResultReceptionCI {

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
	 * @throws Exception Si une erreur se produit lors de la création du port.
	 */
	public MapReduceResultReceptionOutboundPort(ComponentI owner) throws Exception {
		super(MapReduceResultReceptionCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade tous deux jouant le role
		// de client
		assert owner != null;
	}

	/**
	 * Crée et initialise un port sortant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param uri   URI du port sortant.
	 * @param owner Composant propriétaire du port.
	 * @throws Exception Si une erreur se produit lors de la création du port.
	 */
	public MapReduceResultReceptionOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, MapReduceResultReceptionCI.class, owner);

		assert uri != null && owner != null;
	}

	/**
	 * Accepte un résultat d'une opération MapReduce effectuée sur un autre nœud.
	 * 
	 * Cette méthode est appelée pour recevoir le résultat d'une opération
	 * MapReduce. Le résultat peut être intermédiaire ou final, et il est transmis
	 * via un connecteur à un autre composant du système.
	 * 
	 * @param computationURI L'URI de la computation MapReduce.
	 * @param emitterId      L'ID de l'émetteur du résultat.
	 * @param acc            L'accumulateur contenant le résultat du calcul.
	 * @throws Exception Si une erreur se produit lors de la transmission du
	 *                   résultat.
	 */
	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) throws Exception {
		((MapReduceResultReceptionCI) this.getConnector()).acceptResult(computationURI, emitterId, acc);

	}
}
