package pt.iscte.pcd.directory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Directory {
  private ServerSocket serverSocket;

  
  public class TrataCliente extends Thread {
    private Socket socketCliente;
    private String endereco;
    private int portoCliente;

    
    public TrataCliente(Socket socketCliente) {
      this.socketCliente = socketCliente;
    }
    
    public void run() {
       
    	try {
        BufferedReader in = new BufferedReader(new InputStreamReader(
              this.socketCliente.getInputStream()));
        PrintWriter out = new PrintWriter(new BufferedWriter(
              new OutputStreamWriter(this.socketCliente.getOutputStream())), true);
        String msgInicial = in.readLine();
        String[] componentesMensagem = msgInicial.split(" ");
        if (componentesMensagem.length < 2 || !componentesMensagem[0].equals("INSC")) {
          System.err.println("Erro ao receber inscrição do cliente: mensagem inválida" + msgInicial);
          return;
        } 
         endereco = componentesMensagem[1];
         portoCliente = Integer.parseInt(componentesMensagem[2]);
        registaCliente(endereco, portoCliente);
        System.err.println("Cliente inscrito:" + this.socketCliente.getInetAddress().getHostAddress() + " " + portoCliente);
        while (!(socketCliente.isClosed())) {
      	  System.out.println( Directory.this.nos);

          String msg = in.readLine();
          
          if(msg == null) {
        	  this.socketCliente.close();
        	  System.out.println("Closing");
        	  apagaCliente(endereco,portoCliente);
        	  System.out.println( Directory.this.nos);
          }
         
          
          System.err.println("Mensagem recebida: " + msg);
          String str1;
          switch ((str1 = msg).hashCode()) {
            case 104993457:
              if (!str1.equals("nodes"))
                break; 
              trataConsultaClientes(out);
              continue;
          } 
          System.err.println("Mensagem de cliente inválida" + msg);
        } 
      } catch (IOException e) {
    	  try {
  			this.socketCliente.close();
  			apagaCliente(endereco,portoCliente);
  			System.out.println("Cliente apagado da lista");
  			

  			
  		} catch (IOException e1) {
  			// TODO Auto-generated catch block
  			e1.printStackTrace();
  		}
    	  System.err.println("Cliente desconectou-se.");
      }
    	     
            
        
      } 
    
    
    
    private void trataConsultaClientes(PrintWriter out) {
      for (int i = 0; i != Directory.this.nos.size(); i++)
        out.println("node " + (String)Directory.this.nos.get(i)); 
      out.println("end");
    }
    
    private void registaCliente(String endereco, int portoCliente) {
      Directory.this.nos.add(String.valueOf(endereco) + " " + portoCliente);
    }

  
   private void apagaCliente(String endereco, int portoCliente) {
	   Directory.this.nos.remove(String.valueOf(endereco) + " " + portoCliente);   }
  }
  
  private ArrayList<String> nos = new ArrayList<>();
  
  public void serve() {
	    System.err.println("Serviço a iniciar...");
	    while (true) {
	      try {
	        while (true) {
	          Socket s = this.serverSocket.accept();
	          (new TrataCliente(s)).start();
	        }
	      } catch (IOException e) {
	        System.err.println("Erro ao aceitar ligação de cliente no diretório.");
	      } 
	    } 
	  }
  
  public Directory(int porto) throws IOException {
    this.serverSocket = new ServerSocket(porto);
  }
  
  public static void main(String[] args) {
    if (args.length != 1)
      throw new RuntimeException("Número do porto deve ser dado como argumento."); 
    try {
      (new Directory(Integer.parseInt(args[0]))).serve();
    } catch (NumberFormatException e) {
      throw new RuntimeException("Número do porto mal formatado: deve ser inteiro");
    } catch (IOException e) {
      throw new RuntimeException("Erro no lançamento. Poderá o porto " + Integer.parseInt(args[0]) + " já estar a ser usado?");
    } 
  }
}
