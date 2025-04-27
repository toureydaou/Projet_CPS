package etape3;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape2.endpoints.DHTServicesEndPoint;
import etape3.composants.AsynchronousNodeBCM;
import etape3.composants.ConcurrentGetClient1;
import etape3.composants.ConcurrentPutClient1;
import etape3.composants.ConcurrentPutClient2;
import etape3.composants.ConcurrentPutClient3;
import etape3.composants.FacadeBCM;
import etape3.composants.GetClient;
import etape3.composants.MapReduceClient;
import etape3.composants.MapReduceVideClient;
import etape3.composants.PutClient;
import etape3.composants.RemoveClient;
import etape3.endpoints.AsynchronousCompositeMapContentEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;
import fr.sorbonne_u.utils.aclocks.ClocksServer;


/**
 * La CVM.
 * 
 * <p>
 * Nous suivons ici la suite d'opérations suivantes:
 * <ul>
 * <li> un client lance un premier map reduce sur toute la table </li>
 * <li> un autre client déclenche ensuite une série d'insertions dans la table </li>
 * <li> un client déclenche après une série de récupérations sur la table </li>
 * <li> deux clients lancent simultanément un map/reduce sur la table </li>
 * <li> un client passe après pour supprimer des données de la table </li>
 * <li> un client insère une donnée tandis qu'un autre tente au même moment de lire la donnée insérée </li>
 * <li> deux clients font des insertions sur la table au même moment</li>
 * </ul>
 * </p>
 * 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class CVM extends AbstractCVM {

	public static final String TEST_CLOCK_URI = "test-clock";

	public static final Instant START_INSTANT = Instant.now();

	protected static final long START_DELAY = 3000L;

	public static final double ACCELERATION_FACTOR = 60.0;

	protected static final String FACADE_COMPONENT_URI = "facade-URI";

	protected static final String MAP_REDUCE_VIDE_CLIENT_COMPONENT_URI = "map-reduce-vide-client-URI";

	protected static final String GET_1_CLIENT_COMPONENT_URI = "get-1-client-URI";

	protected static final String GET_2_CLIENT_COMPONENT_URI = "get-2-client-URI";

	protected static final String CONCURRENT_GET_1_CLIENT_COMPONENT_URI = "concurrent-get-1-client-URI";

	protected static final String CONCURRENT_PUT_1_CLIENT_COMPONENT_URI = "concurrent-put-1-client-URI";

	protected static final String CONCURRENT_PUT_2_CLIENT_COMPONENT_URI = "concurrent-put-2-client-URI";

	protected static final String CONCURRENT_PUT_3_CLIENT_COMPONENT_URI = "concurrent-put-3-client-URI";

	protected static final String PUT_CLIENT_COMPONENT_URI = "put-client-URI";

	protected static final String REMOVE_CLIENT_COMPONENT_URI = "remove-client-URI";

	protected static final String MAP_REDUCE_1_CLIENT_COMPONENT_URI = "map-reduce-1-client-URI";

	protected static final String MAP_REDUCE_2_CLIENT_COMPONENT_URI = "map-reduce-2-client-URI";

	protected static final String FIRST_NODE_COMPONENT_URI = "first-node--URI";

	protected static final String SECOND_NODE_COMPONENT_URI = "second-node-URI";

	protected static final String THIRD_CLIENT_COMPONENT_URI = "third-node-URI";

	/**
	 * Crée la CVM
	 *
	 * @throws Exception the exception
	 */
	public CVM() throws Exception {
		super();
	}

	/**
	 * 
	 * @see fr.sorbonne_u.components.cvm.AbstractCVM#deploy()
	 */
	@Override
	public void deploy() throws Exception {
		assert !this.deploymentDone();

		long unixEpochStartTimeInNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() + START_DELAY);

		String clockServerURI = AbstractComponent.createComponent(ClocksServer.class.getCanonicalName(),
				new Object[] { TEST_CLOCK_URI, unixEpochStartTimeInNanos, START_INSTANT, ACCELERATION_FACTOR });

		assert this.isDeployedComponent(clockServerURI);

		// endpoint client - facade
		DHTServicesEndPoint dhtServicesEndPoint = new DHTServicesEndPoint();

		// endpoint facade - 1er noeud
		AsynchronousCompositeMapContentEndPoint asynchronousCompositeMapContentEndpointFacadeToFirstNode = new AsynchronousCompositeMapContentEndPoint();

		// endpoint 1er noeud - 2eme noeud
		AsynchronousCompositeMapContentEndPoint asynchronousCompositeMapContentEndpointOnetoTwo = new AsynchronousCompositeMapContentEndPoint();

		// endpoint 2eme noeud - 3eme noeud
		AsynchronousCompositeMapContentEndPoint asynchronousCompositeMapContentEndpointTwotoThree = new AsynchronousCompositeMapContentEndPoint();

		// creation composant facade
		String facadeURI = AbstractComponent.createComponent(FacadeBCM.class.getCanonicalName(), new Object[] {
				FACADE_COMPONENT_URI,
				((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointFacadeToFirstNode)
						.copyWithSharable(),
				((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(facadeURI);

		String mapReduceVideClientURI = AbstractComponent.createComponent(MapReduceVideClient.class.getCanonicalName(),
				new Object[] { MAP_REDUCE_VIDE_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(mapReduceVideClientURI);

		String getClientURI_1 = AbstractComponent.createComponent(GetClient.class.getCanonicalName(), new Object[] {
				GET_1_CLIENT_COMPONENT_URI, ((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(getClientURI_1);

		String putClientURI = AbstractComponent.createComponent(PutClient.class.getCanonicalName(), new Object[] {
				PUT_CLIENT_COMPONENT_URI, ((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(putClientURI);

		String mapReduce1ClientURI = AbstractComponent.createComponent(MapReduceClient.class.getCanonicalName(),
				new Object[] { MAP_REDUCE_1_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(mapReduce1ClientURI);

		String mapReduce2ClientURI = AbstractComponent.createComponent(MapReduceClient.class.getCanonicalName(),
				new Object[] { MAP_REDUCE_2_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(mapReduce2ClientURI);

		String removeClientURI = AbstractComponent.createComponent(RemoveClient.class.getCanonicalName(), new Object[] {
				REMOVE_CLIENT_COMPONENT_URI, ((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });
		assert this.isDeployedComponent(removeClientURI);

		String concurrentGetClientURI = AbstractComponent.createComponent(ConcurrentGetClient1.class.getCanonicalName(),
				new Object[] { CONCURRENT_GET_1_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert this.isDeployedComponent(concurrentGetClientURI);

		String concurentPutClientURI_1 = AbstractComponent.createComponent(
				ConcurrentPutClient1.class.getCanonicalName(), new Object[] { CONCURRENT_PUT_1_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert this.isDeployedComponent(concurentPutClientURI_1);

		String concurentPutClientURI_2 = AbstractComponent.createComponent(
				ConcurrentPutClient2.class.getCanonicalName(), new Object[] { CONCURRENT_PUT_2_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert this.isDeployedComponent(concurentPutClientURI_2);

		String concurentPutClientURI_3 = AbstractComponent.createComponent(
				ConcurrentPutClient3.class.getCanonicalName(), new Object[] { CONCURRENT_PUT_3_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert this.isDeployedComponent(concurentPutClientURI_3);

		// création premier noeud
		String firstNodeURI = AbstractComponent.createComponent(AsynchronousNodeBCM.class.getCanonicalName(),
				new Object[] { FIRST_NODE_COMPONENT_URI,
						((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointFacadeToFirstNode)
								.copyWithSharable(),
						((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointOnetoTwo)
								.copyWithSharable(),
						new IntInterval(0, 49) });

		assert this.isDeployedComponent(firstNodeURI);

		// création deuxième noeud
		String secondNodeURI = AbstractComponent.createComponent(AsynchronousNodeBCM.class.getCanonicalName(),
				new Object[] { SECOND_NODE_COMPONENT_URI,
						((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointOnetoTwo)
								.copyWithSharable(),
						((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointTwotoThree)
								.copyWithSharable(),
						new IntInterval(50, 99) });

		assert this.isDeployedComponent(secondNodeURI);

		// création troisième noeud
		String thirdNodeURI = AbstractComponent.createComponent(AsynchronousNodeBCM.class.getCanonicalName(),
				new Object[] { THIRD_CLIENT_COMPONENT_URI,
						((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointTwotoThree)
								.copyWithSharable(),
						((AsynchronousCompositeMapContentEndPoint) asynchronousCompositeMapContentEndpointFacadeToFirstNode)
								.copyWithSharable(),
						new IntInterval(100, 149) });

		assert this.isDeployedComponent(thirdNodeURI);

		super.deploy();
		assert this.deploymentDone();
	}

	/**
	 * La méthode main
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
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
