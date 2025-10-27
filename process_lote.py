import classLogger
from conection.busca_dados import selecionar, selecionar_all_dados
from conection import ConectionClass as classConection
from tratamento.prep_campos import set_campos_valores_aquisicao
from conection.transation_status import atualiza_status_processando
from concurrent.futures import ThreadPoolExecutor, as_completed
from requestUrl.request_url import processar_request
import psycopg2  
import time

def processar_lote(self):
    """
    Função responsável por buscar e processar os registros em lote.
    """
    classLogger.logger.info(f"Iniciando processamento de lote ({self.batch_size} registros máx).")
    
    registros = selecionar_all_dados(self)
           
    # classLogger.logger.warn(self.batch_size)
    # classLogger.logger.warn('recebo os dados vindo dao class Processor teste')
    # classLogger.logger.warn(registros)
    
    if not registros:
       classLogger.logger.info("Nenhum registro para processar")
       return 0
        
    classLogger.logger.info(f"Iniciando processamento de {len(registros)} registros")

    registros_preparados = []
    new_registros = []

    with classConection.DbConnect(self.config, auto_commit=False) as conn_status:
         cursor_initil = conn_status.cursor()

         for registro in registros:
              #//* step 2
             registro, erro_preparacao = set_campos_valores_aquisicao(registro)

             if not erro_preparacao:
              
                    #//* step 3
                  
                  registro['new_status'] = 1
                  registro['resposta_json'] = ''
                  registro['sucesso'] = False
                    # registro.update(registro.copy())

                  atualiza_status_processando(self,registro, cursor_initil, conn_status)
                  registros_preparados.append(registro)
             else:
                 classLogger.logger.error(f"Erro na preparação - Transação {registro.get('transacao_id')}")
         conn_status.commit()
         cursor_initil.close()
         time.sleep(0.5)

    classLogger.logger.warn(f"Fase 1 concluída: {len(registros_preparados)} ")
    
    total_processados = 0
    total_processados_info = 0

    conn_status2 = psycopg2.connect(
            host=self.config.HOST,
            port=self.config.PORT,
            database=self.config.DATABASE,
            user=self.config.USER,
            password=self.config.PASSWORD
        )
    conn_status2.autocommit = False
        
    conn_status4 = psycopg2.connect(
            host=self.config.HOST,
            port=self.config.PORT,
            database=self.config.DATABASE,
            user=self.config.USER,
            password=self.config.PASSWORD
     )
    conn_status4.autocommit = False
    
     
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(processar_request, self,registro, conn_status2, conn_status4): registro 
                for registro in registros_preparados
            }
            classLogger.logger.info(f"meus fature {futures}")

            for future in as_completed(futures):
                try:
                    future.result()
                    total_processados += 1
                    total_processados_info += 1
                except Exception as e:
                    classLogger.logger.info(f"meus fature sss{futures[future]}")
                    registro = futures[future]
                    classLogger.logger.error(f"Erro ao processar transação {registro.get('transacao_id')}: {str(e)}")
        
    try:
            conn_status2.commit()
            conn_status4.commit()
    except:
            pass
    finally:
            conn_status2.close()
            conn_status4.close()
            
    classLogger.logger.info(f"Lote concluído: {total_processados} registros processados")

    for i in range(self.batch_size):
        classLogger.logger.debug(f"Processando registro {i+1}")
        total_processados_info += 1

    classLogger.logger.info("Lote processado com sucesso....")
    
    return total_processados
