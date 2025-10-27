from classsProcessor import Processor
import time
from threading import Timer
import classLogger


if __name__ == "__main__":
    instance = Processor(max_workers=10, batch_size=2, idProcesso=338)
    # instance = Processor(max_workers=10, batch_size=75)
    tempo_espera_ciclo = 60  # Tempo de espera (em segundos) entre um ciclo e outro
    
    classLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Iniciando loop contínuo...")

    # Loop Infinito
    while True:
        try:
           
            instance.executar_ciclo()
            
            # Pausa antes de recomeçar
            classLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Aguardando {tempo_espera_ciclo} segundos para o próximo ciclo...")
            time.sleep(tempo_espera_ciclo)

        except KeyboardInterrupt:
            # Permite parar o script com Ctrl+C no terminal
            classLogger.logger.info("\nEncerrando loop por comando do usuário (Ctrl+C).")
            break
        except Exception as e:
            # Lida com erros inesperados e continua o loop
            classLogger.logger.info(f"[{time.strftime('%H:%M:%S')}] Erro inesperado: {e}. Continuará em 30 segundos.")
            time.sleep(tempo_espera_ciclo)