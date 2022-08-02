
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class ReadFilesChunk {

    private static final int HEADER_MAGIC_STRING_SIZE = 4;
    private static final int HEADER_VERSION_SIZE = 1;
    private static final int HEADER_RECORDS_NUMBER_SIZE = 4;

    private static final int RECORD_TYPE_SIZE = 1;
    private static final int RECORD_UNIX_TIMESTAMP_SIZE = 4;
    private static final int RECORD_USER_ID_SIZE = 8;
    private static final int RECORD_AMOUNT_SIZE = 8;
  public static void main(String [] pArgs) throws FileNotFoundException, IOException {
    String fileName = "txnlog.dat";
    File file = new File(fileName);
   
    try (InputStream fileInputStream = new FileInputStream(file)) {
      
      byte[] buffer;      
      int bytesRead;

      
      buffer = new byte[HEADER_MAGIC_STRING_SIZE];
      bytesRead = fileInputStream.read(buffer);

      String s = new String(buffer);
      
      System.out.println(s);
      

     

      buffer = new byte[HEADER_VERSION_SIZE];
      bytesRead = fileInputStream.read(buffer);

      BigInteger version = new BigInteger(buffer);

      System.out.println(version);


     

      buffer = new byte[HEADER_RECORDS_NUMBER_SIZE];
      bytesRead = fileInputStream.read(buffer);

      BigInteger recordCount = new BigInteger(buffer);

      System.out.println("RecordCount: " + recordCount);

      double creditsTotal = 0.0;
      double debitsTotal = 0.0;
      int autopayStartCount = 0;
      int autopayEndCount = 0;
      double userAmount = 0.0;

      long userID = 2456938384156277127L;


      while(bytesRead != -1) {

        buffer = new byte[RECORD_TYPE_SIZE];
        bytesRead = fileInputStream.read(buffer);

        BigInteger recordType = new BigInteger(buffer);

        if(recordType.intValue() == 2) {
          autopayStartCount++;
        }
        if(recordType.intValue() == 3) {
          autopayEndCount++;
        }

        System.out.println("Record Type: " + recordType.intValue());

        buffer = new byte[RECORD_UNIX_TIMESTAMP_SIZE];
        bytesRead = fileInputStream.read(buffer);

        BigInteger recordTimestamp = new BigInteger(buffer);

        System.out.println("Timestamp: " + recordTimestamp);
        
        buffer = new byte[RECORD_USER_ID_SIZE];
        bytesRead = fileInputStream.read(buffer);

        BigInteger recordUser = new BigInteger(buffer);

        System.out.println("User ID: " + recordUser);       

        if(recordType.intValue() < 2) {
          buffer = new byte[RECORD_AMOUNT_SIZE];
          bytesRead = fileInputStream.read(buffer);
          
          boolean isUser = false;
          long recordUserLong = recordUser.longValue();
          if(recordUserLong == userID) {            
            isUser = true;
          }
          
          
          ByteBuffer f = ByteBuffer.wrap(buffer);

          double amount = f.getDouble();
          
          if(recordType.intValue() == 0) {
            debitsTotal += amount;
            if(isUser == true) {              
              userAmount -= amount;
            }
            
          }
          else if(recordType.intValue() == 1) {
            creditsTotal += amount;
            if(isUser == true) {              
              userAmount += amount;
            }
            
          }
          
          
          System.out.println("Amount: " + amount);

        }

        System.out.println("\n\n");

      }

      System.out.println("Total credit amount = " + creditsTotal);
      System.out.println("Total debit amount = " + debitsTotal);
      System.out.println("Autopays Started = " + autopayStartCount);
      System.out.println("Autopays Ended = " + autopayEndCount);
      System.out.println();
      System.out.println("Balance for user " + userID + " = " + userAmount);


      
      
    }
  }
}

