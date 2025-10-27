from conection.conexao import conexao
from tabulate import tabulate



def process():
    
    context_conection = conexao()
    
    if context_conection is None:
          return
    cursor = context_conection.cursor()

    dados = ("""
             SELECT p.processo_id,p.data_cadastro,p.data_finalizacao,
	p.finalizado, 
    p.data_cadastro + interval '3 days' < now() as iniciado_a_mais_de_tres_dias,
	p.error,COUNT(t.transacao_id) AS qt_registros_total,
	COALESCE(SUM(CASE WHEN t.status = 3 THEN 1 ELSE 0 END), 0) AS qt_registros_finalizados
	FROM 
	progestor.processo as p
    LEFT JOIN progestor.transacao as t on (t.id_processo = p.processo_id)
    WHERE 
	(p.finalizado = false or p.finalizado is null) and
	p.pause = false 
    GROUP BY p.processo_id,	p.data_cadastro,p.data_finalizacao,	p.finalizado,p.error
    ORDER BY p.processo_id desc;""")
    cursor.execute(dados)
    result_dados = cursor.fetchall()
    
    # print(result_dados)
    if not result_dados:
           return None
   
    colunas = [desc[0] for desc in cursor.description]
    
    #   linha = result_dados[0]
    registro = [dict(zip(colunas, linha)) for linha in  result_dados]
    
    # print(registro)
    
    tabela = tabulate(result_dados,headers=colunas,tablefmt="grid")
    
    # print(tabela)
    
    return registro,colunas
  


 