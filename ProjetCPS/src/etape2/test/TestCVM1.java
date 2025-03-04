package etape2.test;

import etape2.composants.FacadeBCM;
import etape2.composants.NodeBCM;
import etape2.endpoints.CompositeMapContentEndpoint;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class TestCVM1 extends AbstractCVM {

	protected static final String FACADE_COMPONENT_URI = "facade-URI";
	protected static final String CLIENT_1_COMPONENT_URI = "client-1-URI";
	protected static final String CLIENT_2_COMPONENT_URI = "client-2-URI";
	protected static final String CLIENT_3_COMPONENT_URI = "client-3-URI";
	protected static final String FIRST_NODE_COMPONENT_URI = "first-node--URI";
	protected static final String SECOND_NODE_COMPONENT_URI = "second-node-URI";
	protected static final String THIRD_CLIENT_COMPONENT_URI = "third-node-URI";

	public TestCVM1() throws Exception {
		super();

	}

	@Override
	public void deploy() throws Exception {
		assert !this.deploymentDone();

//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.LIFE_CYCLE);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.INTERFACES);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.PORTS);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CONNECTING);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CALLING);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.EXECUTOR_SERVICES);

		// endpoint clients - facade
		DHTServicesEndPoint dhtServicesEndPoint = new DHTServicesEndPoint();

		// endpoint facade - 1er noeud
		CompositeMapContentEndpoint compositeMapContentEndpointFacadeToFirstNode = new CompositeMapContentEndpoint();

		// endpoint 1er noeud - 2eme noeud
		CompositeMapContentEndpoint compositeMapContentEndpointOnetoTwo = new CompositeMapContentEndpoint();

		// endpoint 2eme noeud - 3eme noeud
		CompositeMapContentEndpoint compositeMapContentEndpointTwotoThree = new CompositeMapContentEndpoint();

		// creation composant facade
		String facadeURI = AbstractComponent.createComponent(FacadeBCM.class.getCanonicalName(),
				new Object[] { FACADE_COMPONENT_URI,
						((CompositeMapContentEndpoint) compositeMapContentEndpointFacadeToFirstNode).copyWithSharable(),
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(facadeURI);

		this.toggleTracing(facadeURI);
		this.toggleLogging(facadeURI);

		// création du premier client
		String client_1_URI = AbstractComponent.createComponent(PutClientTest.class.getCanonicalName(), new Object[] {
				CLIENT_1_COMPONENT_URI, ((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert this.isDeployedComponent(client_1_URI);

		this.toggleTracing(client_1_URI);
		this.toggleLogging(client_1_URI);

		// création du premier noeud
		String firstNodeURI = AbstractComponent.createComponent(NodeBCM.class.getCanonicalName(),
				new Object[] { FIRST_NODE_COMPONENT_URI,
						((CompositeMapContentEndpoint) compositeMapContentEndpointFacadeToFirstNode).copyWithSharable(),
						((CompositeMapContentEndpoint) compositeMapContentEndpointOnetoTwo).copyWithSharable(),
						new IntInterval(0, 49) });

		assert this.isDeployedComponent(firstNodeURI);

		this.toggleTracing(firstNodeURI);
		this.toggleLogging(firstNodeURI);

		// création du deuxième noeud
		String secondNodeURI = AbstractComponent.createComponent(NodeBCM.class.getCanonicalName(),
				new Object[] { SECOND_NODE_COMPONENT_URI,
						((CompositeMapContentEndpoint) compositeMapContentEndpointOnetoTwo).copyWithSharable(),
						((CompositeMapContentEndpoint) compositeMapContentEndpointTwotoThree).copyWithSharable(),
						new IntInterval(50, 99) });

		assert this.isDeployedComponent(secondNodeURI);

		this.toggleTracing(secondNodeURI);
		this.toggleLogging(secondNodeURI);

		/*
		 * Le dernier noeud de notre système communique avec le premier noeud en
		 * partageant le même endpoint que la facade
		 */

		// création du troisème noeud
		String thirdNodeURI = AbstractComponent.createComponent(NodeBCM.class.getCanonicalName(),
				new Object[] { THIRD_CLIENT_COMPONENT_URI,
						((CompositeMapContentEndpoint) compositeMapContentEndpointTwotoThree).copyWithSharable(),
						((CompositeMapContentEndpoint) compositeMapContentEndpointFacadeToFirstNode).copyWithSharable(),
						new IntInterval(100, 149) });

		assert this.isDeployedComponent(thirdNodeURI);

		this.toggleTracing(thirdNodeURI);
		this.toggleLogging(thirdNodeURI);

		super.deploy();
		assert this.deploymentDone();

	}

	public static void main(String[] args) {
		try {

			TestCVM1 a = new TestCVM1();

			a.startStandardLifeCycle(20000L);

			Thread.sleep(5000L);

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
