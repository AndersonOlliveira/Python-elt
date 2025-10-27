import classLogger
from conection.transation_status import up_finish_process
from conection.transation_status import up_status_process
from conection import ConectionClass as classConection
from tratamento.prep_campos import set_campos_valores_finish
# from tratamento.prep_campos import set_campos_valores_stauts
from conection.transation_status import process_finish_all
from concurrent.futures import ThreadPoolExecutor, as_completed
from requestUrl.request_url import processar_request
import psycopg2  
import time
from datetime import datetime



def processar_finish(self):

      
     status_registros = process_finish_all(self)
   
     if not status_registros:
         classLogger.logger.info(f"===" * 80)
         classLogger.logger.info("Nenhum processo encontrado para finalizar")
         classLogger.logger.info(f"===" * 80)
         return 0
    
     registros_up_zero = []
     new_registros = []

     with classConection.DbConnect(self.config, auto_commit=False) as conn_status:
         
         cursor_initil = conn_status.cursor()

         for  registro_status in status_registros:
              #//* step 2
             
              registro_status, erro_preparacao , info_status, qt_status = set_campos_valores_finish(registro_status)
            
            #   classLogger.logger.warn(f'tennho os id do info: {info_status}')
              if  info_status:
                    classLogger.logger.info("==" * 80)
                    # classLogger.logger.info(f"Info dos id dos processos com status 7 a mais de 3 dias ")
                    # classLogger.logger.info(f"ids:: {info_status}")
                    new_registros.append(up_status_process(self,info_status,cursor_initil, conn_status))

                    classLogger.logger.info(f"==" * 80)
                       
                    classLogger.logger.info(f"Processo Atualizado para o Status de 7 para 3 {info_status} , quantidade:  {qt_status}")
                       
                    classLogger.logger.info(f"==" * 80)
              else:
                   classLogger.logger.info(f'Proceso não finalizado {registro_status}')       

              
              if not erro_preparacao:
                 
            #          #/// para processar e finalizar
                 if registro_status.get('errors') is True:
                       
                        classLogger.logger.info(f"==" * 80)
                        classLogger.logger.warn(f'minha quantidade de dados a ser apresentado {len(registro_status)} :: {registro_status}')
              
                        registro_status  = up_finish_process(self,registro_status,cursor_initil, conn_status)
                       
                        registros_up_zero.append(registro_status)
                                
                       
                        classLogger.logger.info(f"==" * 80)
                       
                        classLogger.logger.info(f"Processo finalizado {registro_status} , quantidade:  {len(registro_status)}")
                       
                        classLogger.logger.info(f"==" * 80)
              else:
                   
                   classLogger.logger.info(f'Meus dados a não ser processado {erro_preparacao}')
                   classLogger.logger.info(f"=NÃO ATUALIZEI=")
                   classLogger.logger.info(f'Sem dados para ser marcado como concluído {registro_status}')
                  #  return

               
         conn_status.commit()
      
  
     classLogger.logger.warn(f"Fase 4 concluída Finalizado Processos: {len(registros_up_zero)} ")

     return len(registros_up_zero) , len(new_registros)
    