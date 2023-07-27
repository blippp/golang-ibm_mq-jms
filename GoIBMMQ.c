#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include <cmqc.h>
#include <cmqxc.h>

int IBMMQPut (char *ConnectionName
             ,char *ChannelName
             ,char *SSLCipherSpec
             ,char *KeyRepository
             ,char *QueueManagerName
             ,char *QueueName
             ,char *jms_data
             ,char *usr_data
             ,char *msg_data
             ,char *UserLogin
             ,char *UserPassword
             ,char *ErrorMessage);

int IBMMQGet (char *ConnectionName
             ,char *ChannelName
             ,char *SSLCipherSpec
             ,char *KeyRepository
             ,char *QueueManagerName
             ,char *QueueName
             ,long  msg_count
             ,char *msg_data
             ,char *UserLogin
             ,char *UserPassword
             ,char *ErrorMessage);
  
//==========================================================================================================================

int IBMMQPut (char *ConnectionName
             ,char *ChannelName
             ,char *SSLCipherSpec
             ,char *KeyRepository
             ,char *QueueManagerName
             ,char *QueueName
             ,char *jms_data
             ,char *usr_data
             ,char *msg_data
             ,char *UserLogin
             ,char *UserPassword
             ,char *ErrorMessage) 
{
   MQCNO   Connect_options = {MQCNO_DEFAULT}; 
   MQCD    ClientConn      = {MQCD_CLIENT_CONN_DEFAULT}; 
   MQCHAR  QMName[MQ_Q_MGR_NAME_LENGTH];
   MQHCONN Hcon;  
   MQLONG  CompCode;
   MQLONG  Reason;

   MQSCO SSL_options = {MQSCO_DEFAULT}; 
   MQCSP CSP_options = {MQCSP_DEFAULT}; 

   MQOD    od   = {MQOD_DEFAULT};   
   MQMD    md   = {MQMD_DEFAULT};   
   MQPMO   pmo  = {MQPMO_DEFAULT};  
   MQRFH2  rfh2 = {MQRFH2_DEFAULT}; 

   MQHOBJ  Hobj;                   
   MQLONG  O_options;              
   MQLONG  C_options;             
   MQLONG  OpenCode;              
   MQLONG  CReason;               
   MQLONG  mq_messlen;            

   char    mcd_data[100];         
   MQLONG  mcd_len;
   MQLONG  usr_len;
   MQLONG  msg_len;
   MQLONG  jms_len;
   MQLONG  bufflen; 
   MQLONG  rfh2_len;
   char    *mq_mess_ptr; 
   char    *wrk_ptr;
  
   strcpy(ClientConn.ConnectionName, ConnectionName); 
   strcpy(ClientConn.ChannelName, ChannelName); 
   strcpy(QMName, QueueManagerName); 

   if (strlen(KeyRepository) > 1)
   { 
      ClientConn.Version = MQCD_VERSION_7;
//      memcpy(ClientConn.SSLCipherSpec, SSLCipherSpec, strlen(SSLCipherSpec)); 
//      strcpy(ClientConn.SSLCipherSpec, SSLCipherSpec); 
      strncpy(ClientConn.SSLCipherSpec, SSLCipherSpec, MQ_SSL_CIPHER_SPEC_LENGTH);
//      strcpy(SSL_options.KeyRepository, KeyRepository); 
      strncpy(SSL_options.KeyRepository, KeyRepository, MQ_SSL_KEY_REPOSITORY_LENGTH);
//      strncpy(SSL_options.CertificateLabel, CertificateLabel, MQ_CERT_LABEL_LENGTH);
      Connect_options.SSLConfigPtr = &SSL_options; 
   }
   
   if (strlen(UserLogin) > 1)
   { 
      Connect_options.SecurityParmsPtr = &CSP_options; 
   
      CSP_options.AuthenticationType = MQCSP_AUTH_USER_ID_AND_PWD;
      CSP_options.CSPUserIdPtr = UserLogin;
      CSP_options.CSPUserIdLength = strlen(UserLogin);
      CSP_options.CSPPasswordPtr = UserPassword;
      CSP_options.CSPPasswordLength = strlen(CSP_options.CSPPasswordPtr);
   }

   Connect_options.ClientConnPtr = &ClientConn; 
   Connect_options.Version = MQCNO_VERSION_5; 
  
   memset(ErrorMessage, '\0', strlen(ErrorMessage));

   printf("IBMMQPut start\n"); 

   MQCONNX(QMName, &Connect_options, &Hcon, &CompCode, &CReason);
  
   if (CompCode == MQCC_FAILED) 
   { 
      printf("MQCONNX ended with reason code %d\n", CReason); 
      sprintf(ErrorMessage, "MQCONNX ended with reason code: %ld", CReason);
      return((int)Reason); 
   } 

   printf("Connection Successful\n"); 

   strncpy(od.ObjectName, QueueName, (size_t)MQ_Q_NAME_LENGTH);
   printf("Target queue is %s\n", od.ObjectName);

   O_options = MQOO_OUTPUT + MQOO_FAIL_IF_QUIESCING;
   //O_options = MQOO_INQUIRE + MQOO_FAIL_IF_QUIESCING;
   //O_options = MQOO_INPUT_AS_Q_DEF + MQOO_FAIL_IF_QUIESCING;

   MQOPEN(Hcon, &od, O_options, &Hobj, &OpenCode, &Reason);

   if (Reason != MQRC_NONE)
   {
      printf("MQOPEN ended with reason code %ld\n", Reason);
      sprintf(ErrorMessage, "MQOPEN ended with reason code %ld", Reason);
   }

   if (OpenCode == MQCC_FAILED)
   {
      printf("Unable to open queue for output\n");
   }

   CompCode = OpenCode;

   memcpy(md.Format, MQFMT_RF_HEADER_2, (size_t)MQ_FORMAT_LENGTH);

   if (CompCode != MQCC_FAILED)
   {
      strcpy(mcd_data, "<mcd><Msd>jms_text</Msd></mcd>");  

      if (strlen(msg_data) > 0)
      {
         bufflen = strlen(msg_data);
         if (msg_data[bufflen-1] == '\n')
         {
            msg_data[bufflen-1] = '\0';
         }
      } else msg_data[0] = '\0';    

      usr_len = ((strlen(usr_data)-1)/4)*4 + 4 ;
      mcd_len = ((strlen(mcd_data)-1)/4)*4 + 4 ;
      jms_len = ((strlen(jms_data)-1)/4)*4 + 4 ;

      msg_len = strlen(msg_data);

      printf("msg body length = %ld\n",msg_len);   

      mq_messlen = sizeof(MQRFH2) + mcd_len + usr_len + jms_len + msg_len + sizeof(mcd_len);
      if (jms_len > 0) mq_messlen = mq_messlen + sizeof(jms_len);
      if (usr_len > 0) mq_messlen = mq_messlen + sizeof(usr_len);

      mq_mess_ptr = malloc(mq_messlen); 

      memset(mq_mess_ptr, ' ', mq_messlen);

      wrk_ptr = mq_mess_ptr+sizeof(MQRFH2);  

      memcpy(wrk_ptr, &mcd_len, sizeof(mcd_len));
      wrk_ptr = wrk_ptr + sizeof(mcd_len);       
      memcpy(wrk_ptr, mcd_data, strlen(mcd_data));
      wrk_ptr = wrk_ptr + mcd_len;

      if (jms_len > 0)
      {                      
         memcpy(wrk_ptr, &jms_len, sizeof(jms_len));
         wrk_ptr = wrk_ptr + sizeof(jms_len);       
         memcpy(wrk_ptr, jms_data, strlen(jms_data));
         wrk_ptr = wrk_ptr + jms_len;               
      }
      if (usr_len > 0)
      {                 
         memcpy(wrk_ptr, &usr_len, sizeof(usr_len));
         wrk_ptr = wrk_ptr + sizeof(usr_len);       
         memcpy(wrk_ptr, usr_data, strlen(usr_data));
         wrk_ptr = wrk_ptr + usr_len;               
      }

      memcpy(wrk_ptr, msg_data, strlen(msg_data));
      rfh2_len = wrk_ptr - mq_mess_ptr;  
      rfh2.StrucLength = rfh2_len;       
      memcpy(rfh2.Format, MQFMT_STRING, (size_t)MQ_FORMAT_LENGTH);
      memcpy(mq_mess_ptr, &rfh2, sizeof(MQRFH2));

      if (strlen(msg_data) > 0)
      {
         memcpy(md.MsgId, MQMI_NONE, sizeof(md.MsgId) );
         memcpy(md.CorrelId,       
         MQCI_NONE, sizeof(md.CorrelId) );
 
         md.Encoding = MQENC_NATIVE;
         md.CodedCharSetId = 1208; 
         md.Persistence = 1;
         //md.Expiry = 3000;

         //printf("issuing MQPUT msg with:\n");
         //printf("jms properties:\n%s\n", jms_data);
         //printf("user properties:\n%s\n", usr_data);
         //printf("message:\n%s\n", msg_data);

         MQPUT(Hcon, Hobj, &md, &pmo, mq_messlen, mq_mess_ptr, &CompCode, &Reason);

         if (Reason != MQRC_NONE)
         {
            printf("MQPUT ended with reason code %ld\n", Reason);
            sprintf(ErrorMessage, "MQPUT ended with reason code %ld", Reason);
         }
      } else
      {  
         CompCode = MQCC_FAILED;
         printf("The message text was not entered. MQPUT not issued\n");
      }

      (void)free(mq_mess_ptr);
   }

   if (OpenCode != MQCC_FAILED)
   {
      C_options = 0;
         
      MQCLOSE(Hcon, &Hobj, C_options, &CompCode, &Reason);   

      if (Reason != MQRC_NONE)
      {
         printf("MQCLOSE ended with reason code %ld\n", Reason);
         sprintf(ErrorMessage, "MQCLOSE ended with reason code %ld", Reason);
      }
   } 

   if (CReason != MQRC_ALREADY_CONNECTED)
   {
      MQDISC(&Hcon, &CompCode, &Reason);

      if (Reason != MQRC_NONE)
      {
         printf("MQDISC ended with reason code %ld\n", Reason);
         sprintf(ErrorMessage, "MQDISC ended with reason code %ld", Reason);
      }
   }

   if (strlen(ErrorMessage) == 0) 
   {
      strcpy(ErrorMessage, "ok"); 
   }

   printf("IBMMQPut end\n"); 
   return(0); 
}

//==========================================================================================================================

int IBMMQGet (char *ConnectionName
             ,char *ChannelName
             ,char *SSLCipherSpec
             ,char *KeyRepository
             ,char *QueueManagerName
             ,char *QueueName
             ,long  msg_count
             ,char *msg_data
             ,char *UserLogin
             ,char *UserPassword
             ,char *ErrorMessage) 
{
   MQCNO   Connect_options = {MQCNO_DEFAULT}; 
   MQCD    ClientConn      = {MQCD_CLIENT_CONN_DEFAULT}; 
   MQCHAR  QMName[MQ_Q_MGR_NAME_LENGTH];
   MQHCONN Hcon;  
   MQLONG  CompCode;
   MQLONG  Reason;

   MQSCO   SSL_options     = {MQSCO_DEFAULT}; 
   MQCSP   CSP_options     = {MQCSP_DEFAULT}; 

   MQOD    od  = {MQOD_DEFAULT};
   MQMD    md  = {MQMD_DEFAULT};
   MQGMO   gmo = {MQGMO_DEFAULT};

   MQHOBJ  Hobj;                
   MQLONG  O_options;           
   MQLONG  C_options;           
   MQLONG  OpenCode;            
   MQLONG  CReason;             
   MQLONG  buflen;              
   MQLONG  messlen;             
   int     i;
   char   *pBuffer;            

   strcpy(ClientConn.ConnectionName, ConnectionName); 
   strcpy(ClientConn.ChannelName, ChannelName); 
   strcpy(QMName, QueueManagerName); 

   Connect_options.ClientConnPtr = &ClientConn; 
//*
   if (strlen(KeyRepository) > 1)
   { 
      ClientConn.Version = MQCD_VERSION_7;
      ClientConn.SSLClientAuth = MQSCA_OPTIONAL;
      strncpy(ClientConn.SSLCipherSpec, SSLCipherSpec, MQ_SSL_CIPHER_SPEC_LENGTH);
      strncpy(SSL_options.KeyRepository, KeyRepository, MQ_SSL_KEY_REPOSITORY_LENGTH);
      Connect_options.SSLConfigPtr = &SSL_options; 

      SSL_options.FipsRequired = MQSSL_FIPS_YES;

      SSL_options.Version = MQSCO_VERSION_5;
   }
//*/
//*  
   if (strlen(UserLogin) > 1)
   { 
      Connect_options.SecurityParmsPtr = &CSP_options; 

      CSP_options.AuthenticationType = MQCSP_AUTH_USER_ID_AND_PWD;
      CSP_options.CSPUserIdPtr = UserLogin;
      CSP_options.CSPUserIdLength = strlen(UserLogin);
      CSP_options.CSPPasswordPtr = UserPassword;
      CSP_options.CSPPasswordLength = strlen(CSP_options.CSPPasswordPtr);
   }
//*/
   Connect_options.Version = MQCNO_VERSION_5; 

   memset(ErrorMessage, '\0', strlen(ErrorMessage));
  
   printf("IBMMQGet start\n");

//printf("test %s\n", Connect_options.ClientConnPtr->ChannelName); 

   MQCONNX(QMName, &Connect_options, &Hcon, &CompCode, &CReason);
  
   if (CompCode == MQCC_FAILED) 
   { 
      printf("MQCONNX ended with reason code %d\n", CReason); 
      sprintf(ErrorMessage, "MQCONNX ended with reason code: %ld", CReason);
      return((int)Reason); 
   } 

   printf("Connection Successful\n"); 

   strncpy(od.ObjectName, QueueName, (size_t)MQ_Q_NAME_LENGTH);
   printf("Target queue is %s\n", od.ObjectName);

   //O_options = MQOO_OUTPUT + MQOO_FAIL_IF_QUIESCING;
   //O_options = MQOO_INQUIRE + MQOO_FAIL_IF_QUIESCING;
   O_options = MQOO_INPUT_AS_Q_DEF + MQOO_FAIL_IF_QUIESCING;

   MQOPEN(Hcon, &od, O_options, &Hobj, &OpenCode, &Reason);

   if (Reason != MQRC_NONE)
   {
      printf("MQOPEN ended with reason code %ld\n", Reason);
      sprintf(ErrorMessage, "MQOPEN ended with reason code %ld", Reason);
   }

   if (OpenCode == MQCC_FAILED)
   {
      printf("Unable to open queue for output\n");
   }

   CompCode = OpenCode;

   buflen = 1024 * 1024;
   pBuffer = malloc(buflen); 

   memset(msg_data, '\0', strlen(msg_data));

   while ((CompCode != MQCC_FAILED) && (msg_count != 0))
   {
      memset(pBuffer, '\0', buflen);

      memcpy(md.MsgId, MQMI_NONE, sizeof(md.MsgId));
      memcpy(md.CorrelId, MQCI_NONE, sizeof(md.CorrelId));
      md.Encoding       = MQENC_NATIVE;
      md.CodedCharSetId = MQCCSI_Q_MGR;

      gmo.Options = MQGMO_NO_WAIT + MQGMO_NO_SYNCPOINT + MQGMO_FAIL_IF_QUIESCING;

      MQGET(Hcon, Hobj, &md, &gmo, buflen, pBuffer, &messlen, &CompCode, &Reason);

      if (Reason != MQRC_NONE)
      {
         if (Reason == MQRC_NO_MSG_AVAILABLE)
         {  
            printf("MQGET No more messages\n");
         }
         else       
         {
            printf("MQGET ended with reason code %d\n", Reason);
            sprintf(ErrorMessage, "MQGET ended with reason code %ld", Reason);

            if (Reason == MQRC_TRUNCATED_MSG_FAILED) CompCode = MQCC_FAILED;
         }
      }
      else
      {
         printf("MQGET ended with reason code %d\n", Reason);
//         sprintf(ErrorMessage, "MQGET ended with reason code %ld", Reason);
      }

      if (CompCode != MQCC_FAILED)
      {
         if (Reason != MQRC_NO_MSG_AVAILABLE)
         {
            printf("Message length: %d\n", messlen);
            
            i = 0;
            while(messlen--)
            {
               if (pBuffer[i] == 0)
               {
                  pBuffer[i] = 32;
               }
               i = i + 1;
            }

            pBuffer[i] = '\0';
         } 
      }

//      strncat(msg_data, pBuffer, strlen(pBuffer));
      strcat(msg_data, pBuffer);
      strcat(msg_data, "{#}");
      msg_count = msg_count - 1;
   }

   if (OpenCode != MQCC_FAILED)
   {
      C_options = 0;
      
      MQCLOSE(Hcon, &Hobj, C_options, &CompCode, &Reason);

      if (Reason != MQRC_NONE)
      {
         printf("MQCLOSE ended with reason code %ld\n", Reason);
         sprintf(ErrorMessage, "MQCLOSE ended with reason code %ld", Reason);
      }
   }

   if (CReason != MQRC_ALREADY_CONNECTED)
   {
      MQDISC(&Hcon, &CompCode, &Reason);

      if (Reason != MQRC_NONE)
      {
         printf("MQDISC ended with reason code %ld\n", Reason);
         sprintf(ErrorMessage, "MQDISC ended with reason code %ld", Reason);
      }
   }

   (void)free(pBuffer);

//   printf("msg_data: %s\n", msg_data);

   if (strlen(ErrorMessage) == 0) 
   {
      strcpy(ErrorMessage, "ok"); 
   }
   
   printf("IBMMQGet end\n"); 
   return(0); 
}
