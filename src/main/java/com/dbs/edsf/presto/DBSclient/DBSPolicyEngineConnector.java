package com.dbs.edsf.presto.DBSclient;

//import com.dbs.edsf.tcpclient.DBSNoReachablePdpException;
//import com.dbs.edsf.tcpclient.DBSRetriablePdpConnectionException;
//import com.dbs.edsf.tcpclient.PEException;
//import com.dbs.edsf.tcpclient.TCPClient;
import java.io.IOException;
import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSPolicyEngineConnector {
   private static final String ARCOK = "+ARCOK";
   private static final String AUTH_SUCCESS = "authenticated";
   private static final Logger LOG = LoggerFactory.getLogger(DBSPolicyEngineConnector.class);

//   public String getTrustedUserDetails(TCPClient connection, String userName) throws PEException, IOException {
//      if (userName != null && !userName.isEmpty()) {
//         StringBuilder trustedUserDetails = new StringBuilder();
//         String loginCommand = "verifyusertrust| |" + userName + "";
//         this.sendRequest(connection, loginCommand, trustedUserDetails);
//         String[] results = trustedUserDetails.toString().split("\\|");
//         if (!results[0].startsWith("+ARCOK")) {
//            throw new PEException("Failed to get trusted user details for user: " + userName + "," + trustedUserDetails.toString());
//         } else {
//            return trustedUserDetails.substring("+ARCOK".length());
//         }
//      } else {
//         throw new PEException("userName is either null or empty");
//      }
//   }
//
//   public String getSafeSQL(TCPClient connection, String sUserName, String sDataSource, String sDataBaseName, String sDataBaseSchema, String sQuery) throws IOException, PEException {
//      StringBuilder sSafeSQL = new StringBuilder();
//      sSafeSQL = this.processCommand(connection, sUserName, sDataSource, sDataBaseName, sDataBaseSchema, sQuery, sSafeSQL);
//      return sSafeSQL.toString();
//   }
//
//   public StringBuilder processCommand(TCPClient connection, String sUserName, String sDataSource, String sDataBaseName, String sDataBaseSchema, String sQuery, StringBuilder sSafeSQL) throws IOException {
//      this.processInitCommand(connection, sUserName, sDataSource, sDataBaseName, sDataBaseSchema, sSafeSQL);
//      return this.processQuery(connection, sQuery, sSafeSQL);
//   }
//
//   private void processInitCommand(TCPClient connection, String sUserName, String sDataSource, String sDataBaseName, String sDataBaseSchema, StringBuilder sSafeSQL) throws IOException {
//      String sCommand = null;
//      String sCurrentIP = null;
//      sCommand = "login|-u|" + sUserName + "," + sDataSource + "";
//      this.sendRequest(connection, sCommand, sSafeSQL);
//      if (!sSafeSQL.toString().startsWith("authenticated")) {
//         throw new IOException("User not authorized: " + sUserName);
//      } else {
//         InetAddress ipAddr = InetAddress.getLocalHost();
//         sCurrentIP = ipAddr.getHostAddress();
//         sCommand = "arcentry| |" + connection.getHostAddress() + "," + connection.getPort() + "," + sCurrentIP + ", ," + sDataSource + "," + sCurrentIP + ",,";
//         this.sendRequest(connection, sCommand, sSafeSQL);
//         sCommand = "arcdsqchange|-i|" + sDataSource + "," + sDataBaseName + "";
//         this.sendRequest(connection, sCommand, sSafeSQL);
//         sCommand = "arcschchange|-i|" + sDataSource + "," + sDataBaseName + "," + sDataBaseSchema + "";
//         this.sendRequest(connection, sCommand, sSafeSQL);
//      }
//   }
//
//   private StringBuilder processQuery(TCPClient connection, String sQuery, StringBuilder sSafeSQL) throws IOException {
//      String sCommand = "arcsql| |" + sQuery + "";
//      this.sendRequest(connection, sCommand, sSafeSQL);
//      return sSafeSQL;
//   }
//
//   public StringBuilder sendRequest(TCPClient connection, String sCommand, StringBuilder sBuilder) throws DBSNoReachablePdpException, DBSRetriablePdpConnectionException {
//      connection.Send2Server(sCommand);
//      connection.readFromServer(sBuilder);
//      return sBuilder;
//   }
}
