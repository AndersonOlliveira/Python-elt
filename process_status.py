import classLogger
from conection.transation_status import process_status_five
from conection import ConectionClass as classConection
from tratamento.prep_campos import set_campos_valores_aquisicao
from conection.transation_status import up_status
from concurrent.futures import ThreadPoolExecutor, as_completed
from requestUrl.request_url import processar_request
import psycopg2  
import time





def processar_status(self):

     classLogger.logger.info(f"Iniciando processamento de lote de status ({self.batch_size} registros máx).") 
      
     status_registros = process_status_five(self)
           
     classLogger.logger.warn(f"Status 5 registros: {status_registros}") 

     if not status_registros:
       classLogger.logger.info("Nenhum registro para processar")
       return 0
        
     classLogger.logger.info(f"Iniciando processamento de status {len(status_registros)} registros")

     
     registros_up = []
     new_registros = []

     with classConection.DbConnect(self.config, auto_commit=False) as conn_status:
         cursor_initil = conn_status.cursor()

         for registro_status in status_registros:
              #//* step 2
          classLogger.logging.info(f'registros a ser atualizado:  {registro_status}')

          registro_status['new_status'], registro_status['sucesso'] = 3,True
         
          registro_status  = up_status(self,registro_status,cursor_initil, conn_status)
          registros_up.append(registro_status)

       
         conn_status.commit()
         cursor_initil.close()
         time.sleep(0.5)
 
     classLogger.logger.warn(f"Fase 2 concluída: {len(registros_up)} registros preparados e gravados com status 3 e salvo como sucesso True")
  
     return len(registros_up)
