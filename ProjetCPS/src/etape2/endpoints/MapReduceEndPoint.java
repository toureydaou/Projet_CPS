package etape2.endpoints;

import etape2.connecteurs.MapReduceSyncConnector;
import etape2.ports.MapReduceSyncInboundPort;
import etape2.ports.MapReduceSyncOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractPort;
import fr.sorbonne_u.components.endpoints.BCMEndPoint;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceSyncCI;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

public class MapReduceEndPoint extends BCMEndPoint<MapReduceSyncCI> {
	private static final long serialVersionUID = 1L;

	protected static boolean	implementationInvariants(
			MapReduceEndPoint instance
			)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}
	
	
	protected static boolean	invariants(MapReduceEndPoint instance)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}
	

	
	public				MapReduceEndPoint(
			String inboundPortURI,
			String outboundPortURI)
	{
		super(MapReduceSyncCI.class, MapReduceSyncCI.class,
				inboundPortURI, outboundPortURI);
	}
	
	public				MapReduceEndPoint()
	{
		super(MapReduceSyncCI.class, MapReduceSyncCI.class);
		
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

		MapReduceSyncInboundPort p =
						new MapReduceSyncInboundPort(this.inboundPortURI, c);
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
		assert	MapReduceEndPoint.implementationInvariants(this) :
				new ImplementationInvariantException(
						"implementationInvariants(this)");
		assert	MapReduceEndPoint.invariants(this) :
				new InvariantException("invariants(this)");
		
		return p;
	}
	
	@Override
	protected MapReduceSyncCI		makeOutboundPort(
		AbstractComponent c,
		String outboundPortURI,
		String inboundPortURI
		) throws Exception
	{
		// Preconditions checking
		assert	c != null : new PreconditionException("c != null");

		MapReduceSyncOutboundPort p =
				new MapReduceSyncOutboundPort(outboundPortURI, c);
		p.publishPort();
		c.doPortConnection(
				p.getPortURI(),
				this.inboundPortURI,
				MapReduceSyncConnector.class.getCanonicalName());

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
