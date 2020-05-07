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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class ConnectSQLServer {
    private Connection conn;

    private void dbConnectSectores()
    {
        C_Params p = new C_Params();
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            this.conn = DriverManager.getConnection(p.db_host_sqlsrv_sectores,p.db_userid_sqlsrv_sectores, p.db_password_sqlsrv_sectores);
            //System.out.println("Conectado SQLServer");         
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void dbConnectKonesh()
    {
        C_Params p = new C_Params();
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            this.conn = DriverManager.getConnection(p.db_host_sqlsrv_konesh,p.db_userid_sqlsrv_konesh, p.db_password_sqlsrv_konesh);
            //System.out.println("Conectado SQLServer");         
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void dbConnect212(){
        try{
            C_Params p = new C_Params();
            Class.forName("com.mysql.jdbc.Driver");
            this.conn=DriverManager.getConnection(p.db_host_mysql_212,p.db_userid_mysql_212,p.db_password_mysql_212);
            System.out.println("ConectadoMYSQL 212");
        }catch(Exception e){
            System.out.println("¡ ERROR !"+e.getMessage());
        }
    }
    
   
    public void CnsNewRegistrosCartera(String FolioAux){
       this.dbConnectSectores();
       ConnectMySQL cm = new ConnectMySQL();
       try{           
         if(cm.TruncateCartera()){
            System.out.println("3.- Obteniendo Nuevos Registros de SQLServer");
            /*String queryString = "Select * "+
            
                                 " from tblCobCartera " +
                                 " where " +                                
                                 "	convert(varchar(19),dUsuFecCap,20) > convert(varchar(19),?,20) "+
                                 "  order by " +
                                 " dUsuFecCap asc;";*/
            String queryString = "SELECT *FROM tblCobCartera A WHERE cast(A.dFecFac as date) >= cast(GETDATE() as date);";
            PreparedStatement statement = this.conn.prepareStatement(queryString);
            /*statement.setString(1, FolioAux);            */
            ResultSet rs = statement.executeQuery();
            String values="";
            int c=0;
            System.out.println("4.- Insertando Nuevos Registros .... Espere Por Favor...");
            while (rs.next()) {                
                values = rs.getString(1)+",'"+rs.getString(2)+"',"+
                             rs.getString(3)+",'"+rs.getString(4)+"',"+
                             rs.getString(5)+","+rs.getString(6)+","+
                             rs.getString(7)+",'"+rs.getString(8)+"','"+
                             rs.getString(9)+"',"+rs.getString(10)+","+
                             rs.getString(11)+",'"+rs.getString(12)+"',"+
                             rs.getString(13)+","+rs.getString(14)+","+
                             rs.getString(15)+",'"+rs.getString(16)+"','"+
                             rs.getString(17)+"',"+rs.getString(18)+","+
                             rs.getString(19)+",'"+rs.getString(20);
                cm.InsertNewCartera(rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4),rs.getString(5),
                                  rs.getString(6),rs.getString(7),rs.getString(8),rs.getString(9),rs.getString(10),
                                  rs.getString(11),rs.getString(12),rs.getString(13),rs.getString(14),rs.getString(15),
                                  rs.getString(16),rs.getString(17),rs.getString(18),rs.getString(19),rs.getString(20));   
                c++;
            }
            this.conn.close();
            System.out.println("Total de Registros Insertados: "+c);
         }
       }catch(Exception e){
           System.out.println("¡ ERROR !"+e.getMessage());
       }
    }    
    
    public void CnsNewRegistrosEmision(){
       this.dbConnectKonesh();
       ConnectMySQL cm = new ConnectMySQL();
       try{           
         if(cm.TruncateFacturas()){
            System.out.println("3.- Obteniendo Nuevos Registros de SQLServer");
            String queryString = "Select " +
"					(SUBSTRING(CONVERT(VARCHAR(20),b.FECHAEMISION,20),0,5)+b.FOLIO) as c_v_FolioFactura, " +
"					c.CUSTOMDATA_CLAVECONTRATO AS pk_i_empresa, " +
"					CAST(b.TOTAL AS DECIMAL(12,2)) as n_r_ImporteFactura, " +
"					CONVERT(VARCHAR(20),b.FECHAEMISION,20) as f_d_EmisionFactura, " +
"					CONVERT(VARCHAR(20),(DATEADD(yy, DATEDIFF(yy, 0,  b.FECHAEMISION) + 1, -1)),20) AS f_d_Vencimiento, " +
"					CASE c.CUSTOMDATA_TIPODOC " +
"						WHEN 'FACTURA' THEN 1 " +
"						WHEN 'CARGO' THEN 2 " +
"						WHEN 'CREDITO' THEN 3 " +
"					END as pk_i_tipodoctocobro									\n" +
"					from " +
"					       dbo.CFDI_EMISION b " +
"					  JOIN dbo.CFD_CUSTOM_DATA c on (b.UUID=c.UUID) and (b.IDFACTURA=c.IDFACTURA) " +
"					where " +
"					  b.CANCELADO=0 " +
"				      and c.CUSTOMDATA_TIPODOC<>'PAGO' " +
"				      and CAST(b.FECHAEMISION AS DATE) > = CAST(getdate() AS DATE);";
            PreparedStatement statement = this.conn.prepareStatement(queryString);
            /*statement.setString(1, FolioAux);            */
            ResultSet rs = statement.executeQuery();
            String values="";
            int c=0;
            System.out.println("4.- Insertando Nuevos Registros .... Espere Por Favor...");
            while (rs.next()) {                
                values = rs.getString(1)+",'"+rs.getString(2)+"',"+
                             rs.getString(3)+",'"+rs.getString(4)+"',"+
                             rs.getString(5)+","+rs.getString(6);
                cm.InsertNewFacturas(rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4),rs.getString(5),rs.getString(6));   
                c++;
            }
            
            this.conn.close();
            System.out.println("Total de Registros Insertados: "+c);
         }
       }catch(Exception e){
           System.out.println("¡ ERROR !"+e.getMessage());
       }
    }    
    
    /********************     FOTOS      **********************************/
     public void CnsNewRegistrosFotos(){
       this.dbConnect212();
       ConnectMySQL cm = new ConnectMySQL();
       try{           
         if(cm.TruncateFotos()){
            System.out.println("3.- Obteniendo Nuevos Registros de SQLServer");
            String queryString = "Select idfoto,fotofp from fotos.foto;";
            PreparedStatement statement = this.conn.prepareStatement(queryString);
            /*statement.setString(1, FolioAux);            */
            ResultSet rs = statement.executeQuery();
            String values="";
            int c=0;
            System.out.println("4.- Insertando Nuevos Registros .... Espere Por Favor...");
            while (rs.next()) {                                
                cm.InsertNewFotos(rs.getString(1),rs.getBlob(2));   
                c++;
            }
            
            this.conn.close();
            System.out.println("Total de Registros Insertados: "+c);
         }
       }catch(Exception e){
           System.out.println("¡ ERROR !"+e.getMessage());
       }
    }    
     
    
    /******************************   EMPRESAS *****************************************/
    public void CnsEmpresas(){
       this.dbConnectSectores();
       ConnectMySQL cm = new ConnectMySQL();
       try{           
         if(cm.TruncateEmpresas()){
            System.out.println("3.- Obteniendo Nuevos Registros de SQLServer");
            /*String queryString = "Select * "+
            
                                 " from tblCobCartera " +
                                 " where " +                                
                                 "	convert(varchar(19),dUsuFecCap,20) > convert(varchar(19),?,20) "+
                                 "  order by " +
                                 " dUsuFecCap asc;";*/
            String queryString = "Select *from tblConEmpresas;";
            PreparedStatement statement = this.conn.prepareStatement(queryString);
            /*statement.setString(1, FolioAux);            */
            ResultSet rs = statement.executeQuery();
            String values="";
            int c=0;
            System.out.println("4.- Insertando Nuevos Registros .... Espere Por Favor...");
            while (rs.next()) {                
                values = rs.getString(1)+",'"+rs.getString(2)+"',"+
                             rs.getString(3)+",'"+rs.getString(4)+"',"+
                             rs.getString(5)+","+rs.getString(6)+","+
                             rs.getString(7)+",'"+rs.getString(8)+"','"+
                             rs.getString(9)+"',"+rs.getString(10)+","+
                             rs.getString(11)+",'"+rs.getString(12)+"',"+
                             rs.getString(13)+","+rs.getString(14)+","+
                             rs.getString(15)+",'"+rs.getString(16)+"','"+
                             rs.getString(17)+"',"+rs.getString(18)+","+
                             rs.getString(19)+",'"+rs.getString(20)+","+
                             rs.getString(21)+",'"+rs.getString(22)+","+
                             rs.getString(21)+",'"+rs.getString(22)+","+
                             rs.getString(21)+",'"+rs.getString(22)+","+
                             rs.getString(21)+",'"+rs.getString(22)+","+
                             rs.getString(21)+",'"+rs.getString(22)+","+
                             rs.getString(21)+",'"+rs.getString(22)+","+
                             rs.getString(21)+",'"+rs.getString(22)+","+
                             rs.getString(21)+",'"+rs.getString(22)+",";
                
                cm.InsertNewCartera(rs.getString(1),rs.getString(2),rs.getString(3),rs.getString(4),rs.getString(5),
                                  rs.getString(6),rs.getString(7),rs.getString(8),rs.getString(9),rs.getString(10),
                                  rs.getString(11),rs.getString(12),rs.getString(13),rs.getString(14),rs.getString(15),
                                  rs.getString(16),rs.getString(17),rs.getString(18),rs.getString(19),rs.getString(20));   
                c++;
            }
            
            System.out.println("Total de Registros Insertados: "+c);
         }
       }catch(Exception e){
           System.out.println("¡ ERROR !"+e.getMessage());
       }
    }    
    
    
    
}