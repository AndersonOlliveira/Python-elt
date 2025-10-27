import re
import classLogger
from typing import Dict, List, Optional, Tuple
from datetime import datetime

def prepara_campos(rows):
    resultados = []

    for retorno_dados in rows:
     
     #    print(retorno_dados)
        # Captura valores
        linha = retorno_dados.get("campo_aquisicao") or ""
        campos_aquisicao = retorno_dados.get("campos_aquisicao") or "tcpfcnpj"

    
     #    print(f"\nRecebido campo_aquisicao: {linha}")
     #    print(f"Campos de aquisição: {campos_aquisicao}")

    
        campos = campos_aquisicao.split(",")
        valores = linha.split(";")

        erro = False
        parametros = ""

        # Gera parâmetros da query
        for i in range(min(len(campos), len(valores))):
            cAquisicao = campos[i].strip()
            valor = valores[i].strip()
            try:
                parametros += f"&{cAquisicao}={valor}"
            except Exception as e:
                erro = True
                print(f"Erro ao gerar parâmetros: {e}")

        # Limpa caracteres estranhos (ex: r|)
        campo_aquisicao_limpo = re.sub(r"r\|", " ", linha)

        # Atualiza  novos campos
        row_out = retorno_dados.copy()
        row_out.update({
            "parametros": parametros,
            "campo_aquisicao": campo_aquisicao_limpo,
            "erro": erro,
            "status": 0
        })

        resultados.append(row_out)
        
     #    print(resultados)

    return resultados




def set_campos_valores_aquisicao(registro: Dict) -> Tuple[Dict, bool]:

    linha = registro.get('campo_aquisicao', '') or ''  
    
    campos_aquisicao = registro.get('campos_aquisicao', '') or 'tcpfcnpj'

    classLogger.logger.warning(f"Campos aquisição: {campos_aquisicao}")

    campos = [c.strip() for c in campos_aquisicao.split(',')]
   
    valores = [v.strip() for v in linha.split(';')]
   
    classLogger.logger.warning(f"Meus valores fora do split: {valores}")
   
    erro = False
    parametros = ""

  
    dados_associados = {}
    try:
        for campo, valor in zip(campos, valores):
            dados_associados[campo] = valor.strip()
            
            classLogger.logger.info(f"Associação inicial (zip): {dados_associados}")

        # 2. LÓGICA DE CORREÇÃO PARA CASO DE UM ÚNICO VALOR (Inferência de CPF/CNPJ)
        if len(valores) == 1:
            valor_unico = valores[0].strip()
            
            # Checa se o único valor parece ser um CPF/CNPJ (11 ou 14 dígitos numéricos)
            if valor_unico.isdigit() and len(valor_unico) in [11, 14]:
                
                classLogger.logger.warning(
                    f"Apenas um valor ({valor_unico}) foi enviado. "
                    f"Forçando atribuição para 'tcpfcnpj' para pesquisa."
                )
                
                # Limpa a atribuição incorreta feita pelo zip (ex: tmae -> 33289530604)
                dados_associados.clear()
                
                # Atribui o valor correto ao campo essencial
                dados_associados['tcpfcnpj'] = valor_unico
                
                classLogger.logger.info(f"Resultado corrigido: {dados_associados}")

            else:
                 classLogger.logger.info(
                    "O número de valores é 1, mas não é um CPF/CNPJ. "
                    "Mantendo atribuição original e não é possível pesquisar."
                )

        # 3. CONSTRUÇÃO DA STRING DE PARÂMETROS
        # A string de parâmetros deve começar com o campo essencial (se existir)
        # E só deve incluir campos que estão no dicionário (ou seja, que receberam um valor)
        
        for campo, valor in dados_associados.items():
            # Limpa e sanitiza o valor (caso não tenha sido feito antes)
            valor_limpo = valor.strip()
            
           
            # Assumis a parte '&chave=valor'
            parametros += f"&{campo}={valor_limpo}"
            classLogger.logger.debug(f"Parâmetro adicionado: &{campo}={valor_limpo}")
            

        # 4. REALIZA A ASSOCIACAO DOS DADOS
        
    
        registro['parametros'] = parametros 
       
        registro['status'] = 1 if parametros else 0 
        
        classLogger.logger.info(f"Parâmetros finais gerados: {registro['parametros']}")


    except Exception as e:
        classLogger.logger.error(f"Erro ao gerar parâmetros: {str(e)}")
        erro = True
        registro['erro'] = erro

        
        classLogger.logger.warn(f"meus parametros {registro}")


    return registro, erro




 
def set_campos_valores_finish(registro: Dict) -> Tuple[Dict, bool]:
     
     erro = False
     info_status = ''
     qt_status = ''
     
     inicio = registro['iniciado_a_mais_de_tres_dias']
     
     rTotal = registro['qt_registros_total']
     
     rFinalizado = registro['qt_registros_finalizados']
    
     rErros = registro['qt_registros_erros']

                
     try:
              
       
         if inicio is True and rErros > 0:
        #  if  rErros > 0:
            classLogger.logger.info('==' * 80)
            classLogger.logger.info(f'Ids status 7 encontrados :: {registro['processo_id']}')
            info_status, qt_status = registro['processo_id'], rErros
           
            # registro['new_status'],  registro['sucesso'] = 3,True
            # classLogger.logger.info(f"===" * 80)
            # classLogger.logger.info(f"Registro a ser finalizado em transaco pois esta marcado iniciado a 3 dias e tem processos com status 7 transacao no id {registro['processo_id']}")
        
         
         if rFinalizado == rTotal and rTotal > 0:
            
            registro['data_finalizacao'], registro['new_status'], registro['errors'] = datetime.now(),True,True
            classLogger.logger.info(f"===" * 80)
            classLogger.logger.info(f"REGISTRO  INSERIDO  rTotal={rTotal}, rFinalizado={rFinalizado} , o id {registro['processo_id']}")
            classLogger.logger.info(f"====" * 80)
         else:
             erro = True
             registro['erro'] = erro
            
            #  classLogger.logger.info(f"====" * 80)
            #  classLogger.logger.info(f"REGISTRO NÃO FINALIZADO: rTotal={rTotal}, rFinalizado={rFinalizado} , o id {registro['processo_id']}")
     
     except Exception as e:
            classLogger.logger.info(f"===" * 80)
            classLogger.logger.error(f"Erro ao gerar parâmetros: {str(e)}")
            erro = True
            registro['erro'] = erro

    
     return registro, erro, info_status, qt_status

          


