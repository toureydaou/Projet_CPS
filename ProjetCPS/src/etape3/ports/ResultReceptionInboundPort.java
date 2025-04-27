package etape3.ports;

import java.io.Serializable;

import etape4.policies.ThreadsPolicy;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;

/**
 * La classe <code>ResultReceptionInboundPort</code> implémente un port entrant
 * permettant à un composant serveur de recevoir des résultats d'opérations dans
 * un système distribué, comme un système MapReduce.
 *
 * <p>
 * Ce port est utilisé par un composant serveur pour accepter et traiter les
 * résultats envoyés par d'autres composants ou clients dans le cadre
 * d'opérations distribuées.
 * </p>
 *
 * <p>
 * <strong>Propriétaire du port :</strong> Le propriétaire de ce port est un
 * composant serveur qui accepte les
 * résultats d'opérations.
 * </p>
 *
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class ResultReceptionInboundPort extends AbstractInboundPort implements ResultReceptionCI {
	// -------------------------------------------------------------------------
	// Constantes et variables
	// -------------------------------------------------------------------------

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** The executor index. */
	protected final int executorIndex;

	// -------------------------------------------------------------------------
	// Constructeurs
	// -------------------------------------------------------------------------

	/**
	 * Crée et initialise le port entrant avec le composant propriétaire.
	 * 
	 * @param executorIndex L'index de l'exécuteur utilisé pour gérer les tâches
	 *                      concurrentes.
	 * @param owner         Le composant propriétaire du port, qui doit implémenter
	 *                      <code>ResultReceptionI</code>.
	 * @throws Exception Si une erreur se produit lors de la création du port.
	 */
	public ResultReceptionInboundPort(int executorIndex, ComponentI owner) throws Exception {
		super(ResultReceptionCI.class, owner);

		// le propriétaire de ce port est un noeud jouant le role de serveur
		assert (owner instanceof ResultReceptionI);

		assert owner.validExecutorServiceIndex(executorIndex);

		this.executorIndex = executorIndex;
	}

	/**
	 * Crée et initialise un port entrant avec le composant propriétaire et une URI
	 * donnée.
	 * 
	 * @param uri           L'URI du port entrant.
	 * @param executorIndex L'index de l'exécuteur utilisé pour gérer les tâches
	 *                      concurrentes.
	 * @param owner         Le composant propriétaire du port, qui doit implémenter
	 *                      <code>ResultReceptionI</code>.
	 * @throws Exception Si une erreur se produit lors de la création du port.
	 */
	public ResultReceptionInboundPort(String uri, int executorIndex, ComponentI owner) throws Exception {
		super(uri, ResultReceptionCI.class, owner);

		assert uri != null && (owner instanceof ResultReceptionI);
		assert owner.validExecutorServiceIndex(executorIndex);

		this.executorIndex = executorIndex;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI#acceptResult(java.lang.String,
	 *      java.io.Serializable)
	 */
	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {
		this.getOwner().runTask(ThreadsPolicy.RESULT_RECEPTION_HANDLER_URI, owner -> {
			try {
				((ResultReceptionI) owner).acceptResult(computationURI, result);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

}
