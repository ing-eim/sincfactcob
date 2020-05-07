/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sincroniza_factcob;
/**
 *
 * @author depdes10
 */
public class Sincroniza_FactCob {
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println("HELLO RUN");
        // TODO code application logic here
        ConnectSQLServer cn =new ConnectSQLServer();
        ConnectMySQL cm= new ConnectMySQL();
        cn.CnsNewRegistrosCartera(cm.GetUltRegCartera());
        cn.CnsNewRegistrosEmision();
        cn.CnsNewRegistrosFotos();
        
        
        //cn.dbConnect("jdbc:sqlserver://172.30.1.34", "usr_web","Pb13qr7");
    }
}