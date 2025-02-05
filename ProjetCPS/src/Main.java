
import fr.sorbonne_u.cps.mapreduce.endpoints.POJOContentNodeCompositeEndPoint;
import fr.sorbonne_u.cps.mapreduce.utils.IntInterval;

public class Main {
	public static void main(String[] args){
		
		Node[] noeuds = new Node[4];
		POJOContentNodeCompositeEndPoint[] pojos = new POJOContentNodeCompositeEndPoint[5];
		for(int i = 0; i < pojos.length;i++) {
			pojos[i] = new POJOContentNodeCompositeEndPoint();
		}
//		for(int i=0; i<3; i++) {
//			if(i==0) noeuds[i] = new Node(new IntInterval(i*50, (i+1)*50 -1), pojos[i], pojos[noeuds.length], pojos[i+1]);
//			else noeuds[i] = new Node(new IntInterval(i*50, (i+1)*50 -1), pojos[i], pojos[i+1]);
//		}
		

		
		noeuds[0] = new Node(new IntInterval(0, 49), pojos[0], pojos[4], pojos[1]);
		noeuds[1] = new Node(new IntInterval(50, 99), pojos[1], pojos[2]);
		noeuds[2] = new Node(new IntInterval(100, 149), pojos[2], pojos[3]);
		noeuds[3] = new Node(new IntInterval(150, 199), pojos[3], pojos[4]);
		Facade f = new Facade(pojos[0]);
	
		try {
			EntierKey k = new EntierKey(175); 
			f.put(k, new Livre("faim", 200));
			f.put(new EntierKey(50), new Livre("soif", 100));
			f.put(new EntierKey(22), new Livre("douleur", 50));
			f.put(new EntierKey(53), new Livre("pitié", 20));
			f.put(new EntierKey(102), new Livre("peur", 50));
			System.out.println(f.get(k).getValue("nbPages"));
			
			
			
		
	
			int a = f.mapReduce(i->((int)i.getValue("nbPages"))>0,
					i-> new Livre((String)i.getValue("titre"),(int) i.getValue("nbPages")/2),
					(acc, i) ->  (acc + (int)i.getValue("nbPages")),
					(acc, i) ->  (acc + i),
					0);
			System.out.println(a);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}