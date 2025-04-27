package etape4;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import etape2.endpoints.DHTServicesEndPoint;
import etape3.composants.ConcurrentGetClient1;
import etape3.composants.ConcurrentPutClient1;
import etape3.composants.ConcurrentPutClient2;
import etape3.composants.ConcurrentPutClient3;
import etape3.composants.GetClient;
import etape3.composants.MapReduceClient;
import etape3.composants.MapReduceVideClient;
import etape3.composants.PutClient;
import etape3.composants.RemoveClient;
import etape4.composants.DynamicNodeBCM;
import etape4.composants.FacadeBCM;
import etape4.composants.ForceMergeClient;
import etape4.composants.ForceSplitClient;
import etape4.endpoints.CompositeMapContentManagementEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * La CVM.
 * Même scénario de tests que dans l'étape 3 avec
 * de nouvelles opérations pour tester les SPLIT
 * et MERGE
 * 
 * <ul>
 * <li> un client lance un premier map reduce sur toute la table </li>
 * <li> un autre client déclenche ensuite une série d'insertions dans la table </li>
 * <li> un client déclenche après une série de récupérations sur la table </li>
 * <li> deux clients lancent simultanément un map/reduce sur la table </li>
 * <li> un client passe après pour supprimer des données de la table </li>
 * <li> un client insère une donnée tandis qu'un autre tente au même moment de lire la donnée insérée </li>
 * <li> deux clients font des insertions sur la table au même moment </li>
 * <li> des clients font des insertions pour forcer  le déclenchement d'un split ou d'un
 * merge afin de vérifier que l'opération se passe bien </li>
 * 
 * </ul>
 * </p> 
 * @author Touré-Ydaou TEOURI
 * @author Awwal FAGBEHOURO
 */
public class CVM extends AbstractCVM {

	public static final String TEST_CLOCK_URI = "test-clock";
	public static final Instant START_INSTANT = Instant.now();
	protected static final long START_DELAY = 3000L;
	public static final double ACCELERATION_FACTOR = 60.0;
	public static final long CVM_TIME_LIFE_CYCLE = 40000L;

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
	protected static final String FORCE_SPLIT_CLIENT_COMPONENT_URI = "force-split-client-URI";
	protected static final String FORCE_MERGE_CLIENT_COMPONENT_URI = "force-merge-client-URI";
	
	
	protected static final int NUMBER_OF_NODES = 12;
	protected static final int INTERVAL_SIZE = 50;

	public CVM() throws Exception {
		super();
	}

	/**
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

		CompositeMapContentManagementEndPoint[] endpoints = new CompositeMapContentManagementEndPoint[NUMBER_OF_NODES];
		for (int i = 0; i <= NUMBER_OF_NODES -1; i++) {
			endpoints[i] = new CompositeMapContentManagementEndPoint();
		}


		String facadeURI = AbstractComponent.createComponent(FacadeBCM.class.getCanonicalName(), new Object[] {
				FACADE_COMPONENT_URI, endpoints[0].copyWithSharable(), dhtServicesEndPoint.copyWithSharable() });
		
		assert this.isDeployedComponent(facadeURI);
		
		for (int i = 0; i < NUMBER_OF_NODES; i++) {
			
			if (i != NUMBER_OF_NODES -1) {
				String nodeURI = AbstractComponent.createComponent(
						DynamicNodeBCM.class.getCanonicalName(),
					new Object[]{
							AbstractCVM.getThisJVMURI(),
						 "NODE_" + i + "_URI",
						endpoints[i].copyWithSharable(),
						endpoints[i + 1].copyWithSharable(),
						new IntInterval(i * INTERVAL_SIZE, (i + 1) * INTERVAL_SIZE - 1)
					}
				);
				assert this.isDeployedComponent(nodeURI);
			} else  {
				
				String lastNodeURI = AbstractComponent.createComponent(
						DynamicNodeBCM.class.getCanonicalName(),
					new Object[]{
							AbstractCVM.getThisJVMURI(),
						 "NODE_" + i + "_URI",
						endpoints[i].copyWithSharable(),
						endpoints[0].copyWithSharable(),
						new IntInterval(i * INTERVAL_SIZE, (i + 1) * INTERVAL_SIZE - 1)
					}
				);
				assert this.isDeployedComponent(lastNodeURI);
			}
			
			
		}
		

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

		
		String forceSplitClientURI = AbstractComponent.createComponent(
				ForceSplitClient.class.getCanonicalName(), new Object[] { FORCE_SPLIT_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert this.isDeployedComponent(forceSplitClientURI);
		
		
		String forceMergeClientURI = AbstractComponent.createComponent(
				ForceMergeClient.class.getCanonicalName(), new Object[] { FORCE_MERGE_CLIENT_COMPONENT_URI,
						((DHTServicesEndPoint) dhtServicesEndPoint).copyWithSharable() });

		assert this.isDeployedComponent(forceMergeClientURI);
		

		super.deploy();
		assert this.deploymentDone();
	}

	public static void main(String[] args) {
		try {
			// Create an instance of the defined component virtual machine.
			CVM a = new CVM();
			// Execute the application.
			a.startStandardLifeCycle(CVM_TIME_LIFE_CYCLE);
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
