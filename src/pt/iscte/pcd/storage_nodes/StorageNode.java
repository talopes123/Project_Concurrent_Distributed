package pt.iscte.pcd.storage_nodes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import pt.iscte.pcd.CloudByte;

public class StorageNode implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7480751227164702357L;
	/**
	 * 
	 */
	private BufferedReader inDiretorio;
	
	private PrintWriter outDiretorio;
	private Socket socketDiretorio;
	private Socket socket;
	private static int TAMANHO_DADOS = 1000000;
	public static int PORTO_DIRETORIO;
	public static int MEU_PORTO;	
	public static InetAddress endereco;
	private CloudByte [] Inf = new CloudByte [TAMANHO_DADOS];
    private final AtomicBoolean done = new AtomicBoolean(false);
    private static byte[] fileContents;
    private static File file;
    private LinkedList<ByteBlockRequest> lista = new LinkedList<ByteBlockRequest>();
	private Lock l =new ReentrantLock ();
	private ServerSocket ss; 
	private CountDownLatch cl;
	private CountDownLatch cll = new CountDownLatch(2);
	private CountDownLatch clll = new CountDownLatch(1);
	private long startTime;

    
       
    public static class ByteBlockRequest implements Serializable {
    	/**
		 * 
		 */
		private static final long serialVersionUID = 2423126014825719548L;
		private int StartIndex;
    	private int length;
    	
    	public ByteBlockRequest(int StartIndex, int length) {
    		this.StartIndex = StartIndex;
    		this.length = length;
    	}
    	
    	public int getStart() {
    		return StartIndex;
    	}
    	
    	public int getLength() {
    		return length;
    	}
    	
    	public boolean isEqual(ByteBlockRequest b1) {
    		if(b1.getStart() == StartIndex && b1.getLength() == length)
    			return true;
    		return false;
    					
    		
    	}
    	
    	public String toString () {
    		
    		String str = String.valueOf(StartIndex) + " " +  String.valueOf(length);
    		
    		
			return str;
    	}
	}
    
    
    
    
    public StorageNode (File file) {
    	this.file = file;
    	
    }
    
    public StorageNode() {
    	
    }
    
 
	
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException  {
		try {
			endereco = InetAddress.getByName(null);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		file = new File("data.bin");
		
		
		if(args[args.length - 1].equals("data.bin") && file.exists()) {
			PORTO_DIRETORIO = Integer.parseInt(args[args.length -3]);
			MEU_PORTO = Integer.parseInt(args[args.length -2]);
			fileContents = Files.readAllBytes(new File(args[args.length - 1]).toPath());
			
			StorageNode st1 = new StorageNode(file);
			st1.runNodeD();	
		}
		else {
			PORTO_DIRETORIO = Integer.parseInt(args[args.length -2]);
			MEU_PORTO = Integer.parseInt(args[args.length -1]);
			StorageNode st2 = new StorageNode();
			st2.runNodeN();
		}
		
	
	}
			
	
	public void runNodeD() throws InterruptedException, ClassNotFoundException {
		try {
			
			connectToServer();
			
			readData();
			outDiretorio.println("INSC " + endereco + " " + MEU_PORTO);
			
			threadErro();
			
			detetarErro();
			
			System.out.println("Pronto para conexões...");
			startServing();
			
			
				
				
			
			
		} catch (IOException e) {// ERRO...
//		} finally {//a fechar...
//			try {
//				socketDiretorio.close();
//			} catch (IOException e) {//... 
//			}
		}
	}
	
	
	public void criarLista() {
		for(int i = 0; i < 1000000 ; i = i + 100) {
			ByteBlockRequest br = new ByteBlockRequest(i,100);
			lista.add(br);
			

		}
		System.out.println("lista criada");
	}
	
	
	
	public void runNodeN () throws InterruptedException {
		try {
			
			criarLista();

			connectToServer();
			outDiretorio.println("INSC " + endereco + " " + MEU_PORTO);
			outDiretorio.println("nodes");
			
			Descarregar();
			startServing();
		
	
			
		} catch (IOException e) {// ERRO...
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
//		} finally {//a fechar...
//			try {
//				socketDiretorio.close();
//				
//				//socket.close();
//			} catch (IOException e) {//... 
//			}
			
		}
	}
	

	void connect(int porto) throws IOException {
		
		System.out.println("Endereco:" + endereco);
		this.socket = new Socket(endereco, porto);
		System.out.println("Socket:" + socket);
	//	 ObjectInputStream in = new ObjectInputStream ( this.socket . getInputStream ());
	//	 ObjectOutputStream out =new ObjectOutputStream ( this.socket . getOutputStream ());
	}
	
	
	
		
		
	
	void connectToServer() throws IOException {
		
		System.out.println("Endereco:" + endereco);
		socketDiretorio = new Socket(endereco, PORTO_DIRETORIO);
		System.out.println("Socket:" + socketDiretorio);
		inDiretorio = new BufferedReader ( new InputStreamReader (
				socketDiretorio.getInputStream ()));
		outDiretorio = new PrintWriter(new BufferedWriter(
				new OutputStreamWriter(socketDiretorio.getOutputStream())),
				true);
		
	}
	
	
	private boolean validacaoErro() throws IOException {
		int linhas = 0;
		
		outDiretorio.println("nodes");	
		
		inDiretorio.mark(500);

		while(!inDiretorio.readLine().equals("end")) {
			linhas++;
			System.out.println(linhas);
		}
		
		
		

		inDiretorio.reset();

		System.out.println(linhas);
		
		
		
		if(linhas < 3) {

			return false;}
		
		else {
			return true;
		}
	
				
	}
	
	private void corrigirErro(ByteBlockRequest br) throws IOException {
		
		
		
		System.out.println("A comecar a correcao de erros");
		
		outDiretorio.println("nodes");	
		

		
		while(true) {
		
			

		String str = inDiretorio.readLine();
		
		
		
		if(str.equals("end"))
			break;
			
		System.out.println("Eco:" + str);
		
		
		if (str.contains(String.valueOf(MEU_PORTO)) == false) {
		
		 Thread thread = new Thread(){
			    public void run(){
			    	
			    		System.out.println("Consultando o nó : " + str.split(" ")[1].split("/")[1] + " " +  str.split(" ")[2]);
			    		
			    		Socket socket;
						try {
							socket = new Socket(endereco,Integer.parseInt(str.split(" ")[2]));
				    		 ObjectInputStream in = new ObjectInputStream ( socket . getInputStream ());
				    		 ObjectOutputStream out =new ObjectOutputStream ( socket . getOutputStream ());
				    		
				    		 
				    		 
				    		 
				    		 
				    		 out.writeObject(br);
			    			 out.flush();
			    			 
			    			CloudByte [] cba = (CloudByte[]) in.readObject();	
			    			
			    			 if(!Inf[br.getStart()].isParityOk() && cba[0].isParityOk()) {
			    				 Inf[br.getStart()] = cba[0];
			    				 cll.countDown();
			    				 
			    			 }
			    			 else if(Inf[br.getStart()].isParityOk() && Inf[br.getStart()].equals(cba[0]) && cll.getCount() == 1){
			    				 
			    				 cll.countDown();
			    				 done.set(false);
			    				 System.out.println("Valor corrigido : " + Inf[br.getStart()]);
				    			 cll = new CountDownLatch(2);

			    			 }
			    			 
			    			 
			    			 
			    			 cll.await();
			    			 
			    			   
			    			   in.close();
					    	   out.close();
					    	   socket.close();
					    	   
		    					    		 		 
						} catch (NumberFormatException | IOException | ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
			    		
	  
			    }
		 };
		   thread.start();
		}
		else
			continue;
		
	
	}
		
		
}
	
	private void detetarErro() throws InterruptedException {
		
		System.out.println("Detetando erros...");
		
		
			for(int ii = 0; ii < 2; ii++) {
			
		
			 Thread thread = new Thread(){
				    public void run(){
				    	
				    //	clll.await();
				    	
				    	while (true) {
				    		for(int i = 0; i < Inf.length; i++) {
				    			if(Inf[i].isParityOk())
				    				continue;
				    			else {
				    				try {
				    					
				    					
				    					
				    					
/*Verificação extra do isParityOk para os casos em que o erro é 
	*corrigido por outra thread no momento em que esta passa para o else
 */	
				          Thread.sleep(20000);		
				    	if(!Inf[i].isParityOk() && !done.get()) {
				    		done.set(true);
	    					ByteBlockRequest br = new ByteBlockRequest(i,1);
	    					corrigirErro(br);
	    					}
										
												
				    					
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
				    			}
				    		
				    			
		   }		    	
		}	
	  }	
    };  
         thread.start();
  
	}
}
	
	
	
	
	 public void startServing() throws IOException, ClassNotFoundException {
		
		ss = new ServerSocket(MEU_PORTO);
		
		
		while(true) {
			try {
					
				if(lista.isEmpty()) {
					
				 
				 socket = ss.accept();
				 serve();}
				
				else {
					 try {
		                    cl.await();
		                    long stopTime = System.currentTimeMillis();
				    		long elapsedTime = stopTime - startTime;
				    		System.out.println("Tempo de descarrregamento : " + elapsedTime);
				    		System.out.println("Comprimento dos dados descarregados " + Inf.length);
				    		System.out.println("Pronto para conexões...");
							threadErro();
				    		detetarErro();

		                }
		                catch (InterruptedException e) {
		                    e.printStackTrace();
		                }
				}
				
				
			
		} catch (IOException e) {
	        System.err.println("Erro na conexão entre nós ");
	      
	
		}
	}
		
		
		
	}
		

	
	 private void serve() throws IOException, ClassNotFoundException {
		
		
		 Thread thread = new Thread(){
			    public void run(){
			    		
					
					try {
						
						
						
						ObjectOutputStream out = new ObjectOutputStream( socket.getOutputStream ());
						ObjectInputStream in = new ObjectInputStream( socket.getInputStream ());
						
						
						ByteBlockRequest end = new ByteBlockRequest(0,0);
						while (true) {
						
						 ByteBlockRequest str;
							
						str = (ByteBlockRequest) in.readObject();
						
						
						if(str.isEqual(end)) {
							break;
						}
							
								
						
							
						 int StartIndex = str.getStart();
						 int length = str.getLength();
						
					
						 
						 CloudByte [] cba = new CloudByte [length];
						 
						 
						 for(int i = 0; i < length; i++) {
							
									
						 		cba[i] = Inf[StartIndex + i];
						 		if(Inf[StartIndex + i].isParityOk()) {
						 			continue;
						 		}
				 		
						 		else {
						 			
/*Verificação extra do isParityOk para os casos em que o erro é 
 *                   corrigido por outra thread no momento em que esta passa para o else
  */
						 			
						 			if(!Inf[StartIndex + i].isParityOk() && done.get() == false) {
//						 				
						 				
						 				done.set(true);
						 				corrigirErro(new ByteBlockRequest(str.getStart(), 1));
						 				
//						 				
						    			 cll.await();
//
						    			 cba[i] = Inf[StartIndex + i];
						 				}
						 			
						 	}
						 } 		
						 	
						 
						 		out.writeObject(cba);
						 	
					}
						
			}
					
					catch(EOFException e) {
					    //eof - no error in this case
					}
						 	
					 catch (ClassNotFoundException c) {
						// TODO Auto-generated catch block
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
	
			    
	 }				
   };
   
   			thread.start();
}
//	
	
	
	public void threadErro() throws InterruptedException {		
	     
		 Thread thread = new Thread(){
		    public void run(){
		    	
		    	while(true) {
		    		Scanner scan = new Scanner(System.in);		
		    		String e = scan.next();
		    		int b = scan.nextInt();
		    		
		    		if (e.equals("Error") && b > 0 && b < 1000000 ) {
		    			
		    			
		    			Inf[b].makeByteCorrupt();
		    			System.out.println(Inf[b]);
		    			
//		    			try {
//							if(validacaoErro())
//								clll.countDown();
//							else
//								System.out.println("O minimo de threads necessárias para correção de erros ainda não foi atingido ");
//						} catch (IOException e1) {
//							// TODO Auto-generated catch block
//							e1.printStackTrace();
//						}
		    		
		    		}
		    		else {
		    			System.out.println("Erro inválido");
		    			continue;
		    		}
				}
				
		    }
		  };
		  
			  thread.start();
	  }	
	
	
	public void  readData() throws IOException {
		
			int i = 0;
					
			
			String ByteValue = null;
			
			
			for(byte b : fileContents) {
				
				Inf[i] = new CloudByte(b);
				String a = Inf[i].toString();
				ByteValue = a.replaceAll("[^0-9]", "");
				i++;
			}
			System.out.println("Dados carregados do ficheiro :" + i);
			
			
			
 }
	
	
	public void Descarregar() throws IOException {
		
		int j = 0;
		
		startTime = System.currentTimeMillis();
	
		while (true) {
			
		 
			String str = inDiretorio.readLine();
			
			j++;
			
			if(str.equals("end")) {
			   cl = new CountDownLatch(j - 1);
			  		break;
				}
			
			
			
			
			System.out.println("Eco:" + str);
			
			
			if (str.contains(String.valueOf(MEU_PORTO)) == false) {
			
			 Thread thread = new Thread(){
				    public void run(){
				    	try {
				    		
				    		
				    		Socket socket = new Socket(endereco,Integer.parseInt(str.split(" ")[2]));
				    		
				    		System.out.println("Descarregando do nó : " +  str.split(" ")[1].split("/")[1] + " " +  str.split(" ")[2]);
				    		 ObjectInputStream in = new ObjectInputStream ( socket . getInputStream ());
				    		 ObjectOutputStream out =new ObjectOutputStream ( socket . getOutputStream ());
				    		 int contador = 0;
				    		 
				    		while(lista.isEmpty() == false) {
				    			
				    			ByteBlockRequest pedido;
				    			
				    			l.lock();
				    			
				    			  pedido = lista.poll();
				    			     			
				    			l.unlock();
				    			

				    			 				
				    			out.writeObject(pedido);
			    			    out.flush();
			    			    contador++;
			    			 
			    			int StartIndex = pedido.getStart();
			    			int length = pedido.getLength();
			    			CloudByte [] cba = (CloudByte[]) in.readObject();
			    			
			    				for(int i = 0; i < length; i++) 
			    					  Inf[i + StartIndex] = cba[i];
			    								    			
			    			
				    		}
				    		ByteBlockRequest end = new ByteBlockRequest(0,0);
				    		out.writeObject(end);
				    		out.flush();
				    		
				    		cl.countDown();
				    		Thread.sleep(1000);
				    		System.out.println("A thread " + getId() + " descarregou " + contador + " blocos");
				    		in.close();
				    		out.close();
				    		socket.close();
				    		
				    		
				    		
				    			
				    		cl.countDown();
				    		
				    	} catch (NumberFormatException | IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				    	
				    	
				    	
				    }};
				    thread.start();
			
		 
		}
			else
				continue;
	}
 }
		
}


	
	



