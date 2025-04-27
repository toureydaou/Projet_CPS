package etape3.ports;

import java.io.Serializable;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;


/**
 * La classe <code>ResultReceptionOutboundPort</code> implémente un port sortant
 * permettant à un composant client d'envoyer des résultats d'opérations à un
 * composant serveur dans un système distribué.
 *
 * <p>
 * Ce port est utilisé par un composant client pour envoyer des résultats au serveur
 * dans le cadre d'un système de computation, comme un système MapReduce.
 * </p>
 *
 * <p>
 * <strong>Propriétaire du port :</strong> Le propriétaire de ce port est un composant
 * client qui envoie des résultats de calculs à un serveur ou à un autre composant.
 * </p>
 *
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
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
     * @param owner Le composant propriétaire du port, qui doit être un nœud ou une
     *              façade jouant le rôle de client.
     * @throws Exception Si une erreur se produit lors de la création du port.
     */
	public  ResultReceptionOutboundPort (ComponentI owner) throws Exception {
		super(ResultReceptionCI.class, owner);

		// le propriétaire de ce port est un noeud ou la facade tous deux jouant le role
		// de client
		assert owner != null;
	}

	/**
     * Crée et initialise un port sortant avec le composant propriétaire et une URI donnée.
     * 
     * @param uri L'URI du port sortant.
     * @param owner Le composant propriétaire du port, qui doit être un nœud ou une
     *              façade jouant le rôle de client.
     * @throws Exception Si une erreur se produit lors de la création du port.
     */
	public ResultReceptionOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, ResultReceptionCI.class, owner);

		assert uri != null && owner != null;
	}

	
	 /**
     * Accepte un résultat d'une opération et le transmet au composant serveur pour traitement.
     * 
     * Cette méthode est appelée par le composant client pour envoyer un résultat au
     * serveur. Le résultat est ensuite traité par le serveur.
     * 
     * @param computationURI L'URI de la computation MapReduce.
     * @param result Le résultat de la computation à envoyer.
     * @throws Exception Si une erreur se produit lors de l'envoi du résultat.
     */
	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		((ResultReceptionCI) this.getConnector()).acceptResult(computationURI, result);

	}
}
