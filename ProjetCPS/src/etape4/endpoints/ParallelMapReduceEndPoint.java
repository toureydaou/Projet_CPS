package etape4.endpoints;

import etape4.connectors.ParallelMapReduceConnector;
import etape4.ports.ParallelMapReduceInboundPort;
import etape4.ports.ParallelMapReduceOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Le point d'accès {@code ParallelMapReduceEndPoint} fournit un accès
 * aux opérations map/reduce de la DHT (Distributed Hash Table) pour un composant BCM.
 * <p>
 * Ce point d'accès gère la création des ports entrants et sortants liés à
 * l'interface {@link ParallelMapReduceCI}.
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class ParallelMapReduceEndPoint extends BCMEndPoint<ParallelMapReduceCI> {

	private static final long serialVersionUID = 1L;
	protected int executorServiceIndex;

	protected static boolean implementationInvariants(ParallelMapReduceEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	protected static boolean invariants(ParallelMapReduceEndPoint instance) {
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	/**
	 * crée un endpoint BCM avec URI donnée.
	 *
	 * 
	 * @param inboundPortURI URI du port entrant auquel cet endpoint se connecte.
	 */
	public ParallelMapReduceEndPoint(String inboundPortURI, int executorServiceIndex) {
		super(ParallelMapReduceCI.class, ParallelMapReduceCI.class, inboundPortURI);
		this.executorServiceIndex = executorServiceIndex;
	}

	/**
	 * crée un endpoint BCM.
	 *
	 */
	public ParallelMapReduceEndPoint() {
		super(ParallelMapReduceCI.class, ParallelMapReduceCI.class);

	}

	/**
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeInboundPort(fr.sorbonne_u.components.AbstractComponent, java.lang.String)
	 */
	@Override
	protected AbstractInboundPort makeInboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");
		assert inboundPortURI != null && !inboundPortURI.isEmpty()
				: new PreconditionException("inboundPortURI != null && !inboundPortURI.isEmpty()");

		ParallelMapReduceInboundPort p = new ParallelMapReduceInboundPort(this.inboundPortURI,
				this.executorServiceIndex, c);
		p.publishPort();

		// Postconditions checking
		assert p != null && p.isPublished() : new PostconditionException("return != null && return.isPublished()");
		assert ((AbstractPort) p).getPortURI().equals(inboundPortURI)
				: new PostconditionException("((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert getServerSideInterface().isAssignableFrom(p.getClass())
				: new PostconditionException("getOfferedComponentInterface()." + "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert ParallelMapReduceEndPoint.implementationInvariants(this)
				: new ImplementationInvariantException("implementationInvariants(this)");
		assert ParallelMapReduceEndPoint.invariants(this) : new InvariantException("invariants(this)");

		return p;
	}

	/**
	 * @see fr.sorbonne_u.components.endpoints.BCMEndPoint#makeOutboundPort(fr.sorbonne_u.components.AbstractComponent, java.lang.String)
	 */
	@Override
	protected ParallelMapReduceCI makeOutboundPort(AbstractComponent c, String inboundPortURI) throws Exception {
		// Preconditions checking
		assert c != null : new PreconditionException("c != null");

		ParallelMapReduceOutboundPort p = new ParallelMapReduceOutboundPort(c);
		p.publishPort();
		c.doPortConnection(p.getPortURI(), this.inboundPortURI, ParallelMapReduceConnector.class.getCanonicalName());

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

	public void setExecutorIndex(int executorIndex) {
		this.executorServiceIndex = executorIndex;
	}

}
