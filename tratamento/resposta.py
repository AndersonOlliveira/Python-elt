import json
import re
import classLogger
from typing import Dict, List, Optional, Tuple

def respost_transfor(dados):
    print(dados)
    print('éstou recebendo os dados')
    new =[]
    for registro in dados:
    
        resposta = registro.get('resposta_json', '')
    
    
        resposta_limpa = re.sub(r"[\n\r\t]+", "" ,resposta).strip()
    
        registro['resposta_json'] = resposta_limpa
        new.append(registro.copy())
    
    return new





def limpa_resposta_premium(self, registro: Dict) -> Dict:

            resposta_premium = registro.get('resposta_json', '') or ''

            resposta_premium = re.sub(r'\\n', '', resposta_premium)

            registro['resposta_json'] = resposta_premium
            registro['new_status'] = 2
            registro['sucesso'] = True
            classLogger.logger.debug(f"Resposta premium limpa: {resposta_premium[:100]}...")

            return registro
    