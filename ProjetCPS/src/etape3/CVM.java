package etape3;





import etape2.endpoints.DHTServicesEndPoint;
import etape3.composants.AsynchronousNodeBCM;
import etape3.composants.ClientBCM;
import etape3.composants.FacadeBCM;
import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape3.endpoints.ResultReceptionEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
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

//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.LIFE_CYCLE);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.INTERFACES);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.PORTS);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CONNECTING);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CALLING);
//		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.EXECUTOR_SERVICES);

		
		// endpoint client - facade
		DHTServicesEndPoint dhtServicesEndPoint = new DHTServicesEndPoint();

		// endpoint facade - 1er noeud
		AsynchronousCompositeMapContentEndPoint asynchronousCompositeMapContentEndpointFacadeToFirstNode = new AsynchronousCompositeMapContentEndPoint();

		// endpoint 1er noeud - 2eme noeud
		AsynchronousCompositeMapContentEndPoint asynchronousCompositeMapContentEndpointOnetoTwo = new AsynchronousCompositeMapContentEndPoint();

		// endpoint 2eme noeud - 3eme noeud
		AsynchronousCompositeMapContentEndPoint asynchronousCompositeMapContentEndpointTwotoThree = new AsynchronousCompositeMapContentEndPoint();

		
		// endpoint de retour de resultat vers la facade
		
		ResultReceptionEndPoint rrep = new ResultReceptionEndPoint();
		
		MapReduceResultReceptionEndPoint mapReduceResultReceptionEndPoint = new MapReduceResultReceptionEndPoint();
		
		
		
		
		// creation composant facade
		String facadeURI = AbstractComponent.createComponent(FacadeBCM.class.getCanonicalName(),
				new Object[] { FACADE_COMPONENT_URI,
						((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointFacadeToFirstNode).copyWithSharable(),
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable(), rrep.copyWithSharable(), mapReduceResultReceptionEndPoint.copyWithSharable() });
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
		String firstNodeURI = AbstractComponent.createComponent(AsynchronousNodeBCM.class.getCanonicalName(), new Object[] {
				FIRST_NODE_COMPONENT_URI,
				((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointFacadeToFirstNode).copyWithSharable(),
				((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointOnetoTwo).copyWithSharable(), new IntInterval(0, 49) });
		
		assert	this.isDeployedComponent(firstNodeURI);
		
		this.toggleTracing(firstNodeURI);
		this.toggleLogging(firstNodeURI);
		
		// création deuxième noeud
		String secondNodeURI = AbstractComponent.createComponent(AsynchronousNodeBCM.class.getCanonicalName(), new Object[] {
				SECOND_NODE_COMPONENT_URI,
				((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointOnetoTwo).copyWithSharable(),
				((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointTwotoThree).copyWithSharable(), new IntInterval(50, 99) });

		assert	this.isDeployedComponent(secondNodeURI);
		
		this.toggleTracing(secondNodeURI);
		this.toggleLogging(secondNodeURI);
		
		// création troisième noeud
		String thirdNodeURI = AbstractComponent.createComponent(AsynchronousNodeBCM.class.getCanonicalName(), new Object[] {
				THIRD_CLIENT_COMPONENT_URI,
				((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointTwotoThree).copyWithSharable(),
				((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointFacadeToFirstNode).copyWithSharable(), new IntInterval(100, 149) });

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
			a.startStandardLifeCycle(10000L);
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
