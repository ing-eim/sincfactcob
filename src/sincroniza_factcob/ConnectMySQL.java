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
import java.sql.*;

public class ConnectMySQL{
    private Connection conn;
    private void dbConnect(){
        try{
            C_Params p = new C_Params();
            Class.forName("com.mysql.jdbc.Driver");
            this.conn=DriverManager.getConnection(p.db_host_mysql,p.db_userid_mysql,p.db_password_mysql);
            //System.out.println("ConectadoMYSQL");
        }catch(Exception e){
            System.out.println("¡ ERROR !"+e.getMessage());
        }
    }
    

    
    
   
    /* Selección ultimo regisrto insertado en BD 131 MySQL */
    public String GetUltRegCartera(){
        String FolioAux = "";
        this.dbConnect();
        try{
            Statement stmt=this.conn.createStatement();
            String query = "Select " +
                           "  dUsuFecCap " +
                           "from db_pbi_systemdespol.tblCobCartera " +
                           "order by " +
                           "     dUsuFecCap desc " +
                           "	limit 1; ";
            ResultSet rs=stmt.executeQuery(query);
            System.out.println("1.- Obteniendo Ultimo Registro ... OK");
            while(rs.next()){                
                FolioAux = rs.getString(1);
            }
            System.out.println("... ... Ultimo Registro .. "+FolioAux);
            this.conn.close();
        }catch(Exception w){
            System.out.println("¡ ERROR !"+w.getMessage());
        }        
        return FolioAux;
    } 
    
    public boolean TruncateCartera(){
        boolean r = false;        
        this.dbConnect();        
        System.out.println("Vaciando Tabla Cartera en MySQL");
        try{
            Statement stmt=this.conn.createStatement();
            String query = "Delete from db_pbi_systemdespol.tblCobCartera;";            
            stmt.executeUpdate(query);
            r = true;
            this.conn.close();
            System.out.print("... OK\n");
        }catch(SQLException w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
       }       
       return r;
    }
    
    
    public boolean TruncateFacturas(){
        boolean r = false;        
        this.dbConnect();        
        System.out.println("Vaciando Tabla CFDI en MySQL");
        try{
            Statement stmt=this.conn.createStatement();
            String query = "Delete from db_pbi_systemdespol.o_factura;";            
            stmt.executeUpdate(query);
            r = true;
            this.conn.close();
            System.out.print("... OK\n");
        }catch(SQLException w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
       }       
       return r;
    }
    
    public boolean TruncateDataEmision(){
        boolean r = false;        
        this.dbConnect();        
        System.out.println("Vaciando Tabla CFDI en MySQL");
        try{
            Statement stmt=this.conn.createStatement();
            String query = "Delete from db_pbi_systemdespol.CFD_CUSTOM_DATA;";            
            stmt.executeUpdate(query);
            r = true;
            this.conn.close();
            System.out.print("... OK\n");
        }catch(SQLException w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
       }       
       return r;
    }
    
    
    public boolean TruncateFotos(){
        boolean r = false;        
        this.dbConnect();        
        System.out.println("Vaciando Tabla CFDI en MySQL");
        try{
            Statement stmt=this.conn.createStatement();
            String query = "Delete from db_pbi_systemdespol.o_foto";            
            stmt.executeUpdate(query);
            r = true;
            this.conn.close();
            System.out.print("... OK\n");
        }catch(SQLException w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
       }       
       return r;
    }
    
    
    public boolean TruncateEmpresas(){
        boolean r = false;
        
        this.dbConnect();
        
        System.out.println("Vacia Tabla Empresas en MySQL");
        try{
            Statement stmt=this.conn.createStatement();
            String query = "Delete from db_pbi_systemdespol.tblConEmpresas;";            
            stmt.executeUpdate(query);
            r = true;
            this.conn.close();
            System.out.print("... OK\n");
        }catch(SQLException w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
       }       
       return r;
    }
    
    
    public boolean InsertNewCartera(String nIdNumReg,String nidFolioFac,String tidSector,String nidEmpresa,String nTipoFac,
                                          String cImporteFac,String dFecFac,String dFecVen,String tLineaCapEmi,String tLineaCapPago, 
                                          String nTipoPago,String dFecPago,String nCveBanco,String nCveEntero,String cImportePago,
                                          String tIdenSaldo,String nUsuCap,String dUsuFecCap,String tCheque,String dFecDeposito){    
        boolean r = false;
        this.dbConnect();
        try{
            String query = "Insert into db_pbi_systemdespol.tblCobCartera "
                         + " (nIdNumReg, nidFolioFac, tidSector, nidEmpresa, nTipoFac, " 
                         + "  cImporteFac, dFecFac, dFecVen, tLineaCapEmi, tLineaCapPago, "
                         + "  nTipoPago, dFecPago, nCveBanco, nCveEntero, cImportePago, "
                         + "  tIdenSaldo, nUsuCap, dUsuFecCap, tCheque, dFecDeposito) "+                    
                           " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); ";            
            PreparedStatement stmt = this.conn.prepareStatement(query);
            
            stmt.setString(1,nIdNumReg);
            stmt.setString(2,nidFolioFac);
            stmt.setString(3,tidSector);
            stmt.setString(4,nidEmpresa);
            stmt.setString(5,nTipoFac);
            stmt.setString(6,cImporteFac);
            stmt.setString(7,dFecFac);
            stmt.setString(8,dFecVen);
            stmt.setString(9,tLineaCapEmi);
            stmt.setString(10,tLineaCapPago);
            stmt.setString(11,nTipoPago);
            stmt.setString(12,dFecPago);
            stmt.setString(13,nCveBanco);
            stmt.setString(14,nCveEntero);            
            stmt.setString(15,cImportePago);
            stmt.setString(16,tIdenSaldo);
            stmt.setString(17,nUsuCap);
            stmt.setString(18,dUsuFecCap);
            stmt.setString(19,tCheque);
            stmt.setString(20,dFecDeposito);            
            stmt.executeUpdate();
            r = true;            
            this.conn.close();  
        }catch(Exception w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
        }        
        return r;
    }
    
    public boolean InsertNewFacturas(String c_v_FolioFactura,String nCveEmpresa, String n_r_ImporteFactura,String f_d_EmisionFactura,String f_d_Vencimiento,String pk_i_tipodoctocobro){    
        boolean r = false;
        this.dbConnect();
        try{
            String query = "Insert into db_pbi_systemdespol.o_factura "
                         + " (c_v_FolioFactura,nCveEmpresa,n_r_ImporteFactura,f_d_EmisionFactura,f_d_Vencimiento,pk_i_tipodoctocobro) "+                    
                           " values (?,?,?,?,?,?); ";            
            PreparedStatement stmt = this.conn.prepareStatement(query);
            
            stmt.setString(1,c_v_FolioFactura);
            stmt.setString(2,nCveEmpresa);
            stmt.setString(3,n_r_ImporteFactura);
            stmt.setString(4,f_d_EmisionFactura);
            stmt.setString(5,f_d_Vencimiento);
            stmt.setString(6,pk_i_tipodoctocobro);            
            stmt.executeUpdate();
            r = true;            
            this.conn.close();  
        }catch(Exception w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
        }        
        return r;
    }
    
    
    public boolean InsertNewFotos(String nIdNumEmp, Blob foto){    
        boolean r = false;
        this.dbConnect();
        try{
            String query = "Insert into db_pbi_systemdespol.o_foto "
                         + " (nIdNumEmp,bfoto) "+                    
                           " values (?,?); ";            
            PreparedStatement stmt = this.conn.prepareStatement(query);
            
            stmt.setString(1,nIdNumEmp);
            stmt.setBlob(2,foto);            
            stmt.executeUpdate();
            r = true;            
            this.conn.close();  
        }catch(Exception w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
        }        
        return r;
    }
    
    
    
    
    public boolean EjecutaSincronizacion(){
        boolean r = false;
        this.dbConnect();
        try{
            String query = "{call db_pbi_servmed.SPInsertaLicMed(?)}";
            CallableStatement stmt;
            stmt = this.conn.prepareCall(query);
            stmt.setInt(1, 0);/*Es un Parametro de E/S sin ninguna funcion valida al momento*/
            System.out.println("5.- Sincronizando Registros ... ");
            stmt.executeUpdate();
            r = true;             
            this.conn.close();  
            System.out.print("... OK");
        }catch(SQLException w){
            System.out.println("¡ ERROR !"+w.getMessage());
            r = false;
        }        
        
        return r;
    }
}
