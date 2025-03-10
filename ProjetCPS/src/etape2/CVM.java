package etape2;



import etape2.composants.ClientBCM;
import etape2.composants.FacadeBCM;
import etape2.composants.NodeBCM;
import etape2.endpoints.CompositeMapContentSyncEndpoint;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.helpers.CVMDebugModes;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class CVM extends AbstractCVM {

	protected static final String FACADE_COMPONENT_URI = "facade-URI";
	protected static final String CLIENT_COMPONENT_URI = "client-URI";
	protected static final String FIRST_NODE_COMPONENT_URI = "first-node--URI";
	protected static final String SECOND_NODE_COMPONENT_URI = "second-node-URI";
	protected static final String THIRD_CLIENT_COMPONENT_URI = "third-node-URI";

	public CVM() throws Exception {
		super();
	}

	@Override
	public void deploy() throws Exception {
		assert !this.deploymentDone();

		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.LIFE_CYCLE);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.INTERFACES);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.PORTS);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CONNECTING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CALLING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.EXECUTOR_SERVICES);

		
		// endpoint client - facade
		DHTServicesEndPoint dhtServicesEndPoint = new DHTServicesEndPoint();

		// endpoint facade - 1er noeud
		CompositeMapContentSyncEndpoint compositeMapContentEndpointFacadeToFirstNode = new CompositeMapContentSyncEndpoint();

		// endpoint 1er noeud - 2eme noeud
		CompositeMapContentSyncEndpoint compositeMapContentEndpointOnetoTwo = new CompositeMapContentSyncEndpoint();

		// endpoint 2eme noeud - 3eme noeud
		CompositeMapContentSyncEndpoint compositeMapContentEndpointTwotoThree = new CompositeMapContentSyncEndpoint();

		// creation composant facade
		String facadeURI = AbstractComponent.createComponent(FacadeBCM.class.getCanonicalName(),
				new Object[] { FACADE_COMPONENT_URI,
						((CompositeMapContentSyncEndpoint) compositeMapContentEndpointFacadeToFirstNode).copyWithSharable(),
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert	this.isDeployedComponent(facadeURI);
		
		this.toggleTracing(facadeURI);
		this.toggleLogging(facadeURI);

		// création composant client
		String clientURI = AbstractComponent.createComponent(ClientBCM.class.getCanonicalName(),
				new Object[] { CLIENT_COMPONENT_URI, ((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert	this.isDeployedComponent(clientURI);
		
		this.toggleTracing(clientURI);
		this.toggleLogging(clientURI);
		
		// création premier noeud
		String firstNodeURI = AbstractComponent.createComponent(NodeBCM.class.getCanonicalName(), new Object[] {
				FIRST_NODE_COMPONENT_URI,
				((CompositeMapContentSyncEndpoint) compositeMapContentEndpointFacadeToFirstNode).copyWithSharable(),
				((CompositeMapContentSyncEndpoint) compositeMapContentEndpointOnetoTwo).copyWithSharable(), new IntInterval(0, 49) });
		
		assert	this.isDeployedComponent(firstNodeURI);
		
		this.toggleTracing(firstNodeURI);
		this.toggleLogging(firstNodeURI);
		
		// création deuxième noeud
		String secondNodeURI = AbstractComponent.createComponent(NodeBCM.class.getCanonicalName(), new Object[] {
				SECOND_NODE_COMPONENT_URI,
				((CompositeMapContentSyncEndpoint) compositeMapContentEndpointOnetoTwo).copyWithSharable(),
				((CompositeMapContentSyncEndpoint) compositeMapContentEndpointTwotoThree).copyWithSharable(), new IntInterval(50, 99) });

		assert	this.isDeployedComponent(secondNodeURI);
		
		this.toggleTracing(secondNodeURI);
		this.toggleLogging(secondNodeURI);
		
		// création troisième noeud
		String thirdNodeURI = AbstractComponent.createComponent(NodeBCM.class.getCanonicalName(), new Object[] {
				THIRD_CLIENT_COMPONENT_URI,
				((CompositeMapContentSyncEndpoint) compositeMapContentEndpointTwotoThree).copyWithSharable(),
				((CompositeMapContentSyncEndpoint) compositeMapContentEndpointFacadeToFirstNode).copyWithSharable(), new IntInterval(100, 149) });

		assert	this.isDeployedComponent(thirdNodeURI);
		
		this.toggleTracing(thirdNodeURI);
		this.toggleLogging(thirdNodeURI);
		
		
		super.deploy();
		assert this.deploymentDone();
	}
	
	public static void		main(String[] args)
	{
		try {
			// Create an instance of the defined component virtual machine.
			CVM a = new CVM();
			// Execute the application.
			a.startStandardLifeCycle(20000L);
			// Give some time to see the traces (convenience).
			Thread.sleep(5000L);
			// Simplifies the termination (termination has yet to be treated
			// properly in BCM).
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
