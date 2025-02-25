package etape2.composants;

import java.io.Serializable;

import etape1.EntierKey;
import etape1.Livre;
import etape2.endpoints.DHTServicesEndPoint;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.exceptions.ConnectionException;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentDataI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.content.ContentKeyI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesCI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.frontend.DHTServicesI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.CombinatorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ProcessorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.ReductorI;
import fr.sorbonne_u.cps.dht_mapreduce.interfaces.mapreduce.SelectorI;

@RequiredInterfaces(required= {DHTServicesCI.class})
public class ClientBCM extends AbstractComponent implements DHTServicesI {

	protected DHTServicesEndPoint dsep;
	
	
	protected ClientBCM(String uri, DHTServicesEndPoint dsep) {
		super(uri, 0, 1);
		this.dsep = dsep;
	}

	@Override
	public ContentDataI get(ContentKeyI key) throws Exception {
		return this.dsep.getClientSideReference().get(key);
	}

	@Override
	public ContentDataI put(ContentKeyI key, ContentDataI value) throws Exception {
		return this.dsep.getClientSideReference().put(key, value);
	}

	@Override
	public ContentDataI remove(ContentKeyI key) throws Exception {
		return this.dsep.getClientSideReference().remove(key);
	}

	@Override
	public <R extends Serializable, A extends Serializable> A mapReduce(SelectorI selector, ProcessorI<R> processor,
			ReductorI<A, R> reductor, CombinatorI<A> combinator, A initialAcc) throws Exception {
		return this.dsep.getClientSideReference().mapReduce(selector, processor, reductor, combinator, initialAcc);
	}

	
	
	public void getAndPrintContentData() throws Exception{
		EntierKey k_125 = new EntierKey(125); 
		this.put(k_125, new Livre("famille", 300));
		Livre livre = (Livre) this.get(k_125);
		System.out.println(livre.getValue(Livre.TITRE));
	}
	
	
	public void printMapReduceResult() throws Exception {
		EntierKey k_22 = new EntierKey(22); 
		EntierKey k_120 = new EntierKey(120); 
		this.put(k_22, new Livre("soif", 100));
		this.put(k_120, new Livre("douleur", 50));
		
		int a = this.mapReduce(i->((int)i.getValue(Livre.NB_PAGES))>0,
				i-> new Livre((String)i.getValue(Livre.TITRE),(int) i.getValue(Livre.NB_PAGES)/2),
				(acc, i) ->  (acc + (int)i.getValue(Livre.NB_PAGES)),
				(acc, i) ->  (acc + i),
				0);
		System.out.println("Map reduce " + a);
	}

	@Override
	public  void start() throws ComponentStartException {
		this.logMessage("starting client component.") ;
		super.start() ;
		try {
			if (!dsep.clientSideInitialised()) {
				this.dsep.initialiseClientSide(this);
			}	
		} catch (ConnectionException e) {
			throw new ComponentStartException(e) ;
		}
	}
	
	@Override
	public void	execute() throws Exception
	{
		this.logMessage("executing client component.") ;

		
		this.runTask(
			new AbstractComponent.AbstractTask() {
				@Override
				public void run() {
					try {
						((ClientBCM)this.getTaskOwner()).getAndPrintContentData();
						((ClientBCM)this.getTaskOwner()).getAndPrintContentData();
						((ClientBCM)this.getTaskOwner()).printMapReduceResult();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}) ;
	}
	
	@Override
	public void			finalise() throws Exception
	{
		this.logMessage("stopping client component.") ;
		this.printExecutionLogOnFile("client");
		

		this.dsep.cleanUpClientSide();

	
		super.finalise();
	}

	

	
}
