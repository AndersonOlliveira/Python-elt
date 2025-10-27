from conection.conexao import conexao
from tabulate import tabulate
from typing import Dict, List, Optional, Tuple
import classLogger 
from conection import ConectionClass
from psycopg2.extras import RealDictCursor


import json

def selecionar():
      context_conection = conexao()
      if context_conection is None:
          return

      cursor = context_conection.cursor()

      dados = ("""SELECT p.processo_id,
                    p.contrato,p.rede,p.codcns,p.nome_arquivo,p.aceite_execucao,	
                    p.mensagem_alerta,p.data_cadastro,p.configuracao_json,
                    p.campos_aquisicao,p.loja,p.finalizado,p.data_finalizacao,p.pause,
                    t.transacao_id,t.id_processo,t.campo_aquisicao,t.status,t.sucesso,
	                  t.data_cadastro as data_cadastro_transacao,t.resposta,t.resposta_json 
                    FROM progestor.transacao t INNER JOIN progestor.processo p ON p.processo_id = t.id_processo 
                    WHERE t.status in (0,4) AND (p.finalizado = false OR p.finalizado is null) AND 
                    p.pause = false AND p.error = false AND p.processo_id = 308
            
               ORDER BY random() limit 4;""")
      cursor.execute(dados)
      result_dados = cursor.fetchall()
      if not result_dados:
           return None
      colunas = [desc[0] for desc in cursor.description]
    
    #   linha = result_dados[0]
      registro = [dict(zip(colunas, linha)) for linha in  result_dados]
    
      return registro
#  where p.processo_id =  and t.transacao_id in (2995922,2995923,2995924,2995925,2995926,2995927,2995928,2995929)

    #    print(tabulate(result_dados,headers=colunas,tablefmt="grid"))
    #   print("\n linha ind")
    #   for linha in result_dados:
    #           print(linha)
    #        # transforma em lista de dicionários
        #    registros = [dict(zip(colunas, linha)) for linha in result_dados]
        #    print(json.dumps(registros, indent=4, ensure_ascii=False))
        #    return registros

def selecionar_all_dados(self) -> List[Dict]: 
         

        classLogger.logger.info({self.config})
        classLogger.logger.info({self.idProcesso})
        classLogger.logger.info(f"tamanho da busca {self.batch_size}")
      
        query = ("""SELECT p.processo_id,
                    p.contrato,p.rede,p.codcns,p.nome_arquivo,p.aceite_execucao,	
                    p.mensagem_alerta,p.data_cadastro,p.configuracao_json,
                    p.campos_aquisicao,p.loja,p.finalizado,p.data_finalizacao,p.pause,
                    t.transacao_id,t.id_processo,t.campo_aquisicao,t.status,t.sucesso,
	                  t.data_cadastro as data_cadastro_transacao,t.resposta,t.resposta_json 
                    FROM progestor.transacao t INNER JOIN progestor.processo p ON p.processo_id = t.id_processo 
                    WHERE t.status in (0,4) AND (p.finalizado = false OR p.finalizado is null) AND 
                    p.pause = false AND p.error = false """)
        
        params = []

        if self.idProcesso is not None:
         query += 'AND p.processo_id = %s'
         params.append(self.idProcesso)
                    
        query += " ORDER BY random() LIMIT %s;";
        params.append(self.batch_size)

            
        # classLogger.logger.info(query)
        # classLogger.logger.warn(f"[DEBUG SQL] Query gerada:\n{query}")
        classLogger.logger.warn(f"[DEBUG SQL] Parâmetros: {params}")
     
        with ConectionClass.DbConnect(self.config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, tuple(params))
                registros = cursor.fetchall()
                classLogger.logger.info(f"Capturados {len(registros)} registros para processamento.")
                return [dict(registro) for registro in registros]