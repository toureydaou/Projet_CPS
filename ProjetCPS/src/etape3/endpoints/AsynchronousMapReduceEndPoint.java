package etape3.endpoints;

import etape3.connecteurs.MapReduceConnector;
import etape3.ports.AsynchronousMapReduceInboundPort;
import etape3.ports.AsynchronousMapReduceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * La classe {@code AsynchronousMapReduceEndPoint} implémente un point
 * d'extrémité BCM spécialement conçu pour les calculs asynchrones de type
 * MapReduce.
 * 
 * <p>
 * Elle facilite la création des ports entrants et sortants nécessaires aux
 * communications MapReduce, et les connecte automatiquement via un connecteur
 * pour permettre une exécution asynchrone à travers un service d'exécution
 * dédié.
 * </p>
 * 
 * <p>
 * Cet end-point est destiné aux communications client-serveur dans un contexte
 * MapReduce, où l'exécution asynchrone des tâches est essentielle pour les
 * performances.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 * 
 */
public class AsynchronousMapReduceEndPoint extends BCMEndPoint<MapReduceCI> {

	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

	/** The executor service index. */
	private int executorServiceIndex;

	/**
	 * Implementation invariants.
	 *
	 * @param instance the instance
	 * @return true, if successful
	 */
	protected static boolean implementationInvariants(AsynchronousMapReduceEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	/**
	 * Invariants.
	 *
	 * @param instance the instance
	 * @return true, if successful
	 */
	protected static boolean invariants(AsynchronousMapReduceEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	/**
	 * crée un endpoint BCM avec URI donnée.
	 *
	 * @param executorIndex  the executor index
	 * @param inboundPortURI URI du port entrant auquel cet endpoint se connecte.
	 */
	public AsynchronousMapReduceEndPoint(int executorIndex, String inboundPortURI) {
		super(MapReduceCI.class, MapReduceCI.class, inboundPortURI);
		this.executorServiceIndex = executorIndex;
	}

	/**
	 * crée un endpoint BCM.
	 *
	 */
	public AsynchronousMapReduceEndPoint() {
		super(MapReduceCI.class, MapReduceCI.class);
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeInboundPort(fr.sorbonne_u.components.AbstractComponent,
	 *      java.lang.String)
	 */
	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");
		assert inboundPortURI != null && !inboundPortURI.isEmpty()
				: new PreconditionException("inboundPortURI != null && !inboundPortURI.isEmpty()");

		AsynchronousMapReduceInboundPort p = new AsynchronousMapReduceInboundPort(this.inboundPortURI,
				this.executorServiceIndex, c);
		p.publishPort();

		// Postconditions checking
		assert p != null && p.isPublished() : new PostconditionException("return != null && return.isPublished()");
		assert ((AbstractPort) p).getPortURI().equals(inboundPortURI)
				: new PostconditionException("((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert getServerSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getOfferedComponentInterface()." + "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert AsynchronousMapReduceEndPoint.implementationInvariants(this)
				: new ImplementationInvariantException("implementationInvariants(this)");
		assert AsynchronousMapReduceEndPoint.invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeOutboundPort(fr.sorbonne_u.components.AbstractComponent,
	 *      java.lang.String)
	 */
	@Override
	protected MapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");

		AsynchronousMapReduceOutboundPort p = new AsynchronousMapReduceOutboundPort(c);
		p.publishPort();
		c.doPortConnection(p.getPortURI(), this.inboundPortURI, MapReduceConnector.class.getCanonicalName());

		// Postconditions checking
		assert p != null && p.isPublished() && p.connected()
				: new PostconditionException("return != null && return.isPublished() && " + "return.connected()");
		assert ((AbstractPort) p).getServerPortURI().equals(getInboundPortURI()) : new PostconditionException(
				"((AbstractPort)return).getServerPortURI()." + "equals(getInboundPortURI())");
		assert getClientSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getImplementedInterface().isAssignableFrom(" + "return.getClass())");

		// Invariant checking
		assert implementationInvariants(this) : new ImplementationInvariantException("implementationInvariants(this)");
		assert invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

	/**
	 * Sets the executor index.
	 *
	 * @param executorIndex the new executor index
	 */
	public void setExecutorIndex(int executorIndex) {
		this.executorServiceIndex = executorIndex;
	}

}
