import threading
from datetime import datetime
import time
import classLogger
from process_lote import processar_lote
from process_status import processar_status
from process_status_zero import processar_status_zero
from process_finish import processar_finish
from conection import ConectionClass



class Processor:
    def __init__(self, max_workers: int = 10, batch_size: int = 1000, idProcesso: int | None = None):
        self.config = ConectionClass.DbConfig()
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.idProcesso = idProcesso
        self.servidor = 'proscore.com.br'
        self.batch_counter_status1 = 0
        self.batch_counter_status2 = 0
        self.batch_counter_status4 = 0
        self.lock = threading.Lock()

    def executar(self):
        inicio = datetime.now()
        classLogger.logger.info("=" * 80)
        classLogger.logger.info(f"Iniciando Progestor - Consulta Proscore - {inicio}")
        time.sleep(2)
        classLogger.logger.info("=" * 80)

        try:
            total_processados_lote = processar_lote(self)

            classLogger.logger.info(f"minha quantidade de dados processados{total_processados_lote}")
          
            fim = datetime.now()
            duracao = (fim - inicio).total_seconds()

            classLogger.logger.info("---" * 80)
            classLogger.logger.info(f"Processamento concluído em {duracao:.2f} segundos")
            classLogger.logger.info(f"Total de registros processados: {total_processados_lote}")

        except Exception as e:
            classLogger.logger.error(f"Erro fatal na execução: {str(e)}", exc_info=True)

        finally:
                end_time = datetime.now()
                duration = (end_time - inicio).total_seconds()
                    
                classLogger.logger_finalizar.info("\n" + "=" * 80)
                classLogger.logger_finalizar.info("ESTATÍSTICAS DE EXECUÇÃO DE INSERIR PROCESSOS E REQUEST")
                classLogger.logger_finalizar.info("=" * 80)
                classLogger.logger_finalizar.info(f"Início:                    {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
                classLogger.logger_finalizar.info(f"Quantidade Maxima:         {self.batch_size}")
                classLogger.logger_finalizar.info(f"Fim:                       {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                classLogger.logger_finalizar.info(f"Duração:                   {duration:.2f} segundos")
                classLogger.logger_finalizar.info(f"Quantidade:                {total_processados_lote}")
                # classLogger.logger.info(f"Linhas lidas:              {self.stats['lines_read']}")
                # classLogger.logger.info(f"Linhas filtradas (não):    {self.stats['lines_filtered_out']}")
                # classLogger.logger.info(f"Linhas atualizadas:        {self.stats['lines_updated']}")
                # classLogger.logger.info(f"Linhas escritas:           {self.stats['lines_written']}")
                # classLogger.logger.info(f"Erros:                     {self.stats['errors']}")
                classLogger.logger_finalizar.info("=" * 80)
                 
        pass
    
    def executar_finalizar(self):
         
          inicio = datetime.now()
          classLogger.logger.info("=" * 80)
          classLogger.logger.info(f"Iniciando Progestor - Mudar Status processo Proscore - {inicio}")
          classLogger.logger.info("=" * 80)
          time.sleep(3)
          try:
                total_processados = processar_status(self)

                classLogger.logger.info(f"minha quantidade de dados processados Status 5 registros {total_processados}")
                
                fim = datetime.now()
                duracao = (fim - inicio).total_seconds()

                classLogger.logger.info("---" * 80)
                classLogger.logger.info(f"Processamento concluído em {duracao:.2f} segundos")
                classLogger.logger.info(f"Total de registros processados: {total_processados}")

          except Exception as e:
                classLogger.logger.error(f"Erro fatal na execução: {str(e)}", exc_info=True)
        
          pass
 
    def executar_zero(self):
          inicio = datetime.now()
          classLogger.logger.info("=" * 80)
          classLogger.logger.info(f"Iniciando Progestor - Mudar Status processo Proscore de 0 para 1- {inicio}")
          classLogger.logger.info("=" * 80)
          classLogger.logger.info('ESTOU NO PRCOCESSO ZERO') 
          time.sleep(4)
     
          try:
                total_processados_zero = processar_status_zero(self)

                classLogger.logger.info(f"minha quantidade de dados processados Status 1  {total_processados_zero}")
                
                fim = datetime.now()
                duracao = (fim - inicio).total_seconds()

                classLogger.logger.info("---" * 80)
                classLogger.logger.info(f"Processamento concluído em {duracao:.2f} segundos")
                classLogger.logger.info(f"Total de registros processados: {total_processados_zero}")

          except Exception as e:
                classLogger.logger.error(f"Erro fatal na execução: {str(e)}", exc_info=True)
        
          pass
    
    def executar_finalizar_process(self):
         
        #   logger_finalizar.info('INCIO PROCESSO DE FINALIZAR')
          inicio = datetime.now()
          classLogger.logger.info("=" * 80)
          classLogger.logger.info(f"Pegando Processo Progestor Finalizar, Processos Parados  - {inicio}")
          classLogger.logger.info("=" * 80)
          time.sleep(5)
          try:
                total_processados_finalizado , total_alter_status_seven  = processar_finish(self)
                classLogger.logger.info(f"==" * 80)
                classLogger.logger.info(f"minha quantidade de dados processados para ser Atualizado:  {total_processados_finalizado}")
                
                fim = datetime.now()
                duracao = (fim - inicio).total_seconds()

                classLogger.logger.info("---" * 80)
                classLogger.logger.info(f"Processamento concluído em {duracao:.2f} segundos")
                classLogger.logger.info(f"Total de registros processados Finalizado: {total_processados_finalizado}")
                classLogger.logger.info(f"Total de registros Status Seven alterados: {total_alter_status_seven}")

          except Exception as e:
                classLogger.logger.error(f"Erro fatal na execução: {str(e)}", exc_info=True)
                raise
         
          finally:
                end_time = datetime.now()
                duration = (end_time - inicio).total_seconds()
                    
                classLogger.logger_finalizar.info("\n" + "=" * 80)
                classLogger.logger_finalizar.info("ESTATÍSTICAS DE EXECUÇÃO")
                classLogger.logger_finalizar.info("=" * 80)
                classLogger.logger_finalizar.info(f"Início:                    {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
                classLogger.logger_finalizar.info(f"Fim:                       {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                classLogger.logger_finalizar.info(f"Duração:                   {duration:.2f} segundos")
                classLogger.logger_finalizar.info(f"Quantidade:                {total_processados_finalizado}")
                classLogger.logger_finalizar.info(f"Quantidade alter status seven:                {total_alter_status_seven}")
                # classLogger.logger.info(f"Linhas lidas:              {self.stats['lines_read']}")
                # classLogger.logger.info(f"Linhas filtradas (não):    {self.stats['lines_filtered_out']}")
                # classLogger.logger.info(f"Linhas atualizadas:        {self.stats['lines_updated']}")
                # classLogger.logger.info(f"Linhas escritas:           {self.stats['lines_written']}")
                # classLogger.logger.info(f"Erros:                     {self.stats['errors']}")
                classLogger.logger_finalizar.info("=" * 80)
                    
                pass
    def executar_ciclo(self):
        self.executar()
        self.executar_finalizar()
        self.executar_zero()
        self.executar_finalizar_process()
        classLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Processador {self.idProcesso}: Ciclo completo.")