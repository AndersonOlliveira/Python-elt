import classLogger
from conection.transation_status import process_status_zero
from conection import ConectionClass as classConection
from tratamento.prep_campos import set_campos_valores_aquisicao
from conection.transation_status import up_status
from concurrent.futures import ThreadPoolExecutor, as_completed
from requestUrl.request_url import processar_request
import psycopg2  
import time



def processar_status_zero(self):

     classLogger.logger.info(f"Iniciando processamento de lote de status ({self.batch_size} registros máx).") 
      
     status_registros = process_status_zero(self)
           
     classLogger.logger.warn(self.batch_size)
     classLogger.logger.warn('Recebo os dados e vou realizar para processar e atualizar')
     classLogger.logger.warn(status_registros) 

     if not status_registros:
       classLogger.logger.info("Nenhum registro para processar")
       return 0
        
     classLogger.logger.info(f"Iniciando processamento de status {len(status_registros)} registros")

     
     registros_up_zero = []
     new_registros = []

     with classConection.DbConnect(self.config, auto_commit=False) as conn_status:
         cursor_initil = conn_status.cursor()

         for registro_status in status_registros:
          
          registro_status['new_status'], registro_status['sucesso'] = 0,False
         
          registro_status  = up_status(self,registro_status,cursor_initil, conn_status)
          registros_up_zero.append(registro_status)

       
         conn_status.commit()
         cursor_initil.close()
         time.sleep(0.5)
 
     classLogger.logger.warn(f"Fase 2 concluída: {len(registros_up_zero)} registros preparados e gravados com status 3 e salvo como sucesso True")
     
     return len(registros_up_zero)