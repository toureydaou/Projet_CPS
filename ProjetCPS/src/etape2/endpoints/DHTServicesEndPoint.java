package etape2.endpoints;

import etape2.connecteurs.DHTServicesConnector;
import etape2.ports.DHTServicesInboundPort;
import etape2.ports.DHTServicesOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

public class DHTServicesEndPoint extends BCMEndPoint<DHTServicesCI>  {
	private static final long serialVersionUID = 1L;

	protected static boolean	implementationInvariants(
			DHTServicesEndPoint instance
			)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}
	
	protected static boolean	invariants(DHTServicesEndPoint instance)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}
	
	public				DHTServicesEndPoint(
			String inboundPortURI)
	{
		super(DHTServicesCI.class, DHTServicesCI.class,
				inboundPortURI);
	}
	
	public				DHTServicesEndPoint()
	{
		super(DHTServicesCI.class, DHTServicesCI.class);
	}
	
	@Override
	protected AbstractInboundPort	makeInboundPort(
		AbstractComponent c,
		String inboundPortURI
		) throws Exception
	{
		// Preconditions checking
		assert	c != null : new PreconditionException("c != null");
		assert	inboundPortURI != null && !inboundPortURI.isEmpty() :
				new PreconditionException(
						"inboundPortURI != null && !inboundPortURI.isEmpty()");

		DHTServicesInboundPort p =
						new DHTServicesInboundPort(this.inboundPortURI, c);
		p.publishPort();

		// Postconditions checking
		assert	p != null && p.isPublished() :
				new PostconditionException(
						"return != null && return.isPublished()");
		assert	((AbstractPort)p).getPortURI().equals(inboundPortURI) :
				new PostconditionException(
						"((AbstractPort)return).getPortURI().equals(inboundPortURI)");
		assert	getServerSideInterface().isAssignableFrom(p.getClass()) :
				new PostconditionException(
						"getOfferedComponentInterface()."
						+ "isAssignableFrom(return.getClass())");
		// Invariant checking
		assert	DHTServicesEndPoint.implementationInvariants(this) :
				new ImplementationInvariantException(
						"implementationInvariants(this)");
		assert	DHTServicesEndPoint.invariants(this) :
				new InvariantException("invariants(this)");
		
		return p;
	}
	
	@Override
	protected DHTServicesCI		makeOutboundPort(
		AbstractComponent c,
		String inboundPortURI
		) throws Exception
	{
		// Preconditions checking
		assert	c != null : new PreconditionException("c != null");

		DHTServicesOutboundPort p =
				new DHTServicesOutboundPort(c);
		p.publishPort();
		c.doPortConnection(
				p.getPortURI(),
				this.inboundPortURI,
				DHTServicesConnector.class.getCanonicalName());

		// Postconditions checking
		assert	p != null && p.isPublished() && p.connected() :
				new PostconditionException(
						"return != null && return.isPublished() && "
						+ "return.connected()");
		assert	((AbstractPort)p).getServerPortURI().equals(getInboundPortURI()) :
				new PostconditionException(
						"((AbstractPort)return).getServerPortURI()."
						+ "equals(getInboundPortURI())");
		assert	getClientSideInterface().isAssignableFrom(p.getClass()) :
				new PostconditionException(
						"getImplementedInterface().isAssignableFrom("
						+ "return.getClass())");
		
		// Invariant checking
		assert	implementationInvariants(this) :
				new ImplementationInvariantException(
						"implementationInvariants(this)");
		assert	invariants(this) : new InvariantException("invariants(this)");
		
		return p;
	}
}
