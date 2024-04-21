package pt.iscte.pcd.client;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.font.TextLayout;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import pt.iscte.pcd.CloudByte;
import pt.iscte.pcd.storage_nodes.StorageNode.ByteBlockRequest;

public class Client {	

	
	public static int PORTO_NO;
	private Socket socket;
	public static InetAddress endereco;
	private ObjectInputStream in;
	private ObjectOutputStream out;
	private JTextField pos;
	private JTextField comp; 
	private JTextField field; 
	private JTextArea texto;
	
	
	

	private void criarJanela() {
		
		
JFrame frame = new JFrame("Client");
		
		frame.setSize(900,150);
		
		frame.setLayout(new BorderLayout());
		
		JPanel panel = new JPanel();
		
		panel.setLayout(new FlowLayout());
	
		JLabel label1 = new JLabel("Posiçao a consultar :");
		panel.add(label1);
		frame.add(panel,BorderLayout.NORTH);
		
		pos = new JTextField(10);
		pos.setText("1000");
		panel.add(pos);	
		
		
		JLabel label2 = new JLabel("Comprimento :");
		panel.add(label2);
		
		
		comp = new JTextField(5);
		comp.setText("10");
		panel.add(comp);
		
		
		JPanel panel2 = new JPanel();
		panel.setLayout(new FlowLayout());
		frame.add(panel2);
		
		
		
		String initialText = new String ("Respostas aparecerão aqui... ");
		field = new JTextField(String.valueOf(initialText));
		texto = new JTextArea(initialText,900,80);
		texto.setLineWrap( true );
		
		

		JButton button = new JButton("Consultar");
		button.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				
				try {
					
					
				//	consultarDados(PORTO_NO);
					consultarDados();
					texto.setText(field.getText());
					
				} catch (IOException | ClassNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				//JOptionPane.showMessageDialog(frame, check.isSelected() ? "checked" : "not checked");
				
			}
		});
		
		panel.add(button);	

		
		
		panel2.add(texto);
			
		
		frame.setVisible(true);	
		
	}
	
	
void connect() throws IOException {
		
		
		this.socket = new Socket(endereco, PORTO_NO);
		in = new ObjectInputStream ( this.socket . getInputStream ());
		out =new ObjectOutputStream ( this.socket . getOutputStream ());
	}
	
	
	void consultarDados() throws IOException, ClassNotFoundException {
		
		ByteBlockRequest br = new ByteBlockRequest(Integer.parseInt(pos.getText()), Integer.parseInt(comp.getText()));
		

		connect();
		
		out.writeObject(br);
		out.flush();
		
		int StartIndex = br.getStart();
		int length = br.getLength();
		
		String [] Inf = new String[Integer.parseInt(comp.getText())];
		StringBuffer sb = new StringBuffer();
		CloudByte [] cba = (CloudByte[]) in.readObject();
		
		for(int i = 0; i <  length ; i++) {
			
			//CloudByte cb = (CloudByte) in.readObject();
			Inf[i] = cba[i].toString();
			sb.append(Inf[i]);
			if(cba[i].isParityOk()) {
				sb.append("Parity OK, ");
			}
			else {
				sb.append("Parity NOK, ");
			}
			
			
			
			
			}
		
		field.setText(sb.toString());
		ByteBlockRequest end = new ByteBlockRequest(0,0);
		out.writeObject(end);
		out.flush();
		
		
	}
	
	
	public static void main (String [] args) throws IOException {
		
		endereco = InetAddress.getByName(null);
		
		Client c = new Client();
		
		PORTO_NO = Integer.parseInt(args[args.length -1]);
		c.criarJanela();
		
	}
	

}
