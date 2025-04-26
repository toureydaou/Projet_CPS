package etape4.composants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import etape2.endpoints.DHTServicesEndPoint;
import etape4.CVM;
import etape3.endpoints.MapReduceResultReceptionEndPoint;
import etape3.endpoints.ResultReceptionEndPoint;
import etape4.endpoints.CompositeMapContentManagementEndPoint;
import etape4.policies.IgnoreChordsPolicy;
import etape4.policies.LoadPolicy;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentAccessCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.management.DHTManagementCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.MapReduceResultReceptionI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ParallelMapReduceCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;
import fr.sorbonne_u.cps.mapreduce.utils.URIGenerator;
import fr.sorbonne_u.utils.aclocks.AcceleratedClock;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerConnector;
import fr.sorbonne_u.utils.aclocks.ClocksServerOutboundPort;

@OfferedInterfaces(offered = { DHTServicesCI.class, ResultReceptionCI.class, MapReduceResultReceptionCI.class })
@RequiredInterfaces(required = { ContentAccessCI.class, ParallelMapReduceCI.class, DHTManagementCI.class,
		ClocksServerCI.class })
public class FacadeBCM extends AbstractComponent implements ResultReceptionI, MapReduceResultReceptionI, DHTServicesI {

	protected AcceleratedClock dhtClock; // Référence à l'horloge
	ClocksServerOutboundPort p;

	// URI constants pour l'accès aux services
	private static final String GET_URI_PREFIX = "GET";
	private static final String PUT_URI_PREFIX = "PUT";
	private static final String REMOVE_URI_PREFIX = "REMOVE";
	private static final String MAPREDUCE_URI_PREFIX = "MAPREDUCE";
	private static final String COMPUTE_CHORDS_URI_PREFIX = "COMPUTE-CHORDS";
	private static final String SPLIT_URI_PREFIX = "SPLIT";
	private static final String MERGE_URI_PREFIX = "MERGE";
	private static final int NUMBER_OF_CHORDS = 4;

	private static final int SCHEDULABLE_THREADS = 500;
	private static final int THREADS_NUMBER = 500;

	private static final int START_POLICY = 0;

	private static final int LIMIT_NUMBER_WRITE_OPERATIONS = 30;

	private boolean LIMIT_REACHED = false;

	private AtomicInteger number_write_operation = new AtomicInteger(0);

	protected CompositeMapContentManagementEndPoint endPointFacadeNoeud;
	protected DHTServicesEndPoint endPointClientFacade;
	protected ResultReceptionEndPoint resultatReceptionEndPoint;
	protected MapReduceResultReceptionEndPoint mapReduceResultatReceptionEndPoint;

	private HashMap<String, CompletableFuture<Serializable>> resultsContentAccess;
	private HashMap<String, CompletableFuture<Serializable>> resultsMapReduce;

	protected final ReentrantReadWriteLock splitMergeLock;

	protected FacadeBCM(String uri, CompositeMapContentManagementEndPoint endPointFacadeNoeud,
			DHTServicesEndPoint endPointClientFacade) throws ConnectionException {
		super(uri, THREADS_NUMBER, SCHEDULABLE_THREADS);
		this.endPointFacadeNoeud = endPointFacadeNoeud;
		this.endPointClientFacade = endPointClientFacade;
		this.resultatReceptionEndPoint = new ResultReceptionEndPoint();
		this.mapReduceResultatReceptionEndPoint = new MapReduceResultReceptionEndPoint();
		this.resultsContentAccess = new HashMap<String, CompletableFuture<Serializable>>();
		this.resultsMapReduce = new HashMap<String, CompletableFuture<Serializable>>();
		this.splitMergeLock = new ReentrantReadWriteLock();

		this.endPointClientFacade.initialiseServerSide(this);
		this.resultatReceptionEndPoint.initialiseServerSide(this);
		this.mapReduceResultatReceptionEndPoint.initialiseServerSide(this);

	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		this.splitMergeLock.readLock().lock();
		ContentDataI value;
		try {
			String request_uri = URIGenerator.generateURI(GET_URI_PREFIX);
			System.out.println("Reception de la requête 'GET' sur la facade, identifiant de la requete : " + request_uri);
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			this.resultsContentAccess.put(request_uri, f);
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().get(request_uri, key,
					resultatReceptionEndPoint);
			value = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
			
		}finally{
			this.splitMergeLock.readLock().unlock();
		}
		return value;
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {

		this.splitMergeLock.readLock().lock();
		ContentDataI oldValue;
		try {
			this.countNumberOfOperations();
			String request_uri = URIGenerator.generateURI(PUT_URI_PREFIX);
			System.out.println("Reception de la requête 'PUT' sur la facade identifiant requete : " + request_uri);
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			this.resultsContentAccess.put(request_uri, f);
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().put(request_uri, key, value,
					resultatReceptionEndPoint);
			oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
			this.resultsContentAccess.remove(request_uri);

		}finally{
			this.splitMergeLock.readLock().unlock();
		}

		return oldValue;
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {

		this.splitMergeLock.readLock().lock();
		ContentDataI oldValue;
		try {
			this.countNumberOfOperations();
			String request_uri = URIGenerator.generateURI(REMOVE_URI_PREFIX);
			System.out.println("Reception de la requête 'REMOVE' sur la facade identifiant requete : " + request_uri);
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			this.resultsContentAccess.put(request_uri, f);
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().remove(request_uri, key,
					resultatReceptionEndPoint);
			oldValue = (ContentDataI) this.resultsContentAccess.get(request_uri).get();
			this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(request_uri);
			this.resultsContentAccess.remove(request_uri);

		}finally{
			this.splitMergeLock.readLock().unlock();
		}
		return oldValue;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {

		this.splitMergeLock.readLock().lock();
		A result;
		try {
			String request_uri = URIGenerator.generateURI(MAPREDUCE_URI_PREFIX);
			System.out.println("Reception de la requête 'MAP REDUCE' sur la facade identifiant requete : " + request_uri);
			CompletableFuture<Serializable> reduceResult = new CompletableFuture<Serializable>();
			resultsMapReduce.put(request_uri, reduceResult);

			this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().parallelMap(request_uri, selector,
					processor, new IgnoreChordsPolicy(START_POLICY));
			this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().parallelReduce(request_uri, reductor,
					combinator, initialAcc, initialAcc, new IgnoreChordsPolicy(START_POLICY),
					this.mapReduceResultatReceptionEndPoint);
			result = (A) resultsMapReduce.get(request_uri).get();

			this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference().clearMapReduceComputation(request_uri);
			this.resultsMapReduce.remove(request_uri);
		}finally{
			this.splitMergeLock.readLock().unlock();
		}
		return result;
	}

	public void clearComputation(String computationURI) throws Exception {

		this.endPointFacadeNoeud.getContentAccessEndpoint().getClientSideReference().clearComputation(computationURI);

	}

	public void clearMapReduceComputation(String computationURI) throws Exception {

		this.endPointFacadeNoeud.getMapReduceEndpoint().getClientSideReference()
		.clearMapReduceComputation(computationURI);

	}

	@Override
	public void acceptResult(String computationURI, Serializable result) throws Exception {

		this.resultsContentAccess.get(computationURI).complete(result);
	}

	@Override
	public void acceptResult(String computationURI, String emitterId, Serializable acc) {

		this.resultsMapReduce.get(computationURI).complete(acc);
	}

	public void split() throws Exception {
		this.splitMergeLock.writeLock().lock();
		try {
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			String split_uri = URIGenerator.generateURI(SPLIT_URI_PREFIX);
			this.resultsContentAccess.put(split_uri, f);
			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference().split(split_uri, new LoadPolicy(),
					this.resultatReceptionEndPoint);

			ContentDataI split_response = (ContentDataI) this.resultsContentAccess.get(split_uri).get();
			System.out.println("Fin split");
			this.resultsContentAccess.remove(split_uri);
			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference()
			.computeChords(URIGenerator.generateURI(COMPUTE_CHORDS_URI_PREFIX), NUMBER_OF_CHORDS);
		}finally {
			this.splitMergeLock.writeLock().unlock();
		}


	}

	public void merge() throws Exception {
		this.splitMergeLock.writeLock().lock();
		try {
			CompletableFuture<Serializable> f = new CompletableFuture<Serializable>();
			String merge_uri = URIGenerator.generateURI(MERGE_URI_PREFIX);
			this.resultsContentAccess.put(merge_uri, f);
			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference().merge(merge_uri, new LoadPolicy(),
					this.resultatReceptionEndPoint);
			ContentDataI merge_response = (ContentDataI) this.resultsContentAccess.get(merge_uri).get();
			this.resultsContentAccess.remove(merge_uri);
			this.endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference()
			.computeChords(URIGenerator.generateURI(COMPUTE_CHORDS_URI_PREFIX), NUMBER_OF_CHORDS);
		}finally {
			this.splitMergeLock.writeLock().unlock();
		}

	}

	public void countNumberOfOperations() {
		int count = number_write_operation.incrementAndGet();
		System.out.println("Number op : " + count);
		if ((count != 0) && (count % LIMIT_NUMBER_WRITE_OPERATIONS) == 0) {
			LIMIT_REACHED = true;
		}
	}

	private boolean operationsInProgress() {
		return !this.resultsContentAccess.isEmpty() || !this.resultsMapReduce.isEmpty();
	}

	@Override
	public void start() throws ComponentStartException {
		this.logMessage("starting facade component.");
		super.start();
		try {
			if (!this.endPointFacadeNoeud.clientSideInitialised()) {
				this.endPointFacadeNoeud.initialiseClientSide(this);
			}

		} catch (Exception e) {
			throw new ComponentStartException(e);
		}
	}

	@Override
	public void execute() throws Exception {
		super.execute();

		this.runTask(new AbstractComponent.AbstractTask() {
			@Override
			public void run() {
				try {
					((FacadeBCM) this.taskOwner).endPointFacadeNoeud.getDHTManagementEndpoint().getClientSideReference()
					.computeChords(COMPUTE_CHORDS_URI_PREFIX, NUMBER_OF_CHORDS);

					while (true) {
						Thread.sleep(1000); // pause de 1 seconde

						if (LIMIT_REACHED) {
							if (!((FacadeBCM) this.getTaskOwner()).operationsInProgress()) {
								System.out.println(">> Déclenchement Split/Merge après " + LIMIT_NUMBER_WRITE_OPERATIONS
										+ " opérations");
								((FacadeBCM) this.getTaskOwner()).split();
								((FacadeBCM) this.getTaskOwner()).merge();
								LIMIT_REACHED = false; // seulement après split/merge
							} else {
								System.out.println("Des opérations sont en cours, report du Split/Merge...");
								// On laisse LIMIT_REACHED à true pour réessayer lors de la prochaine itération
							}
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	@Override
	public void finalise() throws Exception {
		this.logMessage("stopping facade component.");
		this.printExecutionLogOnFile("facade");
		this.endPointFacadeNoeud.cleanUpClientSide();
		super.finalise();
	}

	@Override
	public void shutdown() throws ComponentShutdownException {
		try {
			System.out.println("client endpoint shutdown");
			this.endPointClientFacade.cleanUpServerSide();

			System.out.println("resultat endpoint shutdown");
			this.resultatReceptionEndPoint.cleanUpServerSide();

			System.out.println("map resultat endpoint shutdown");
			this.mapReduceResultatReceptionEndPoint.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdown();
	}

	@Override
	public void shutdownNow() throws ComponentShutdownException {
		try {
			this.endPointClientFacade.cleanUpServerSide();
		} catch (Exception e) {
			throw new ComponentShutdownException(e);
		}
		super.shutdownNow();
	}

}
