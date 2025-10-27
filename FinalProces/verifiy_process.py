

def verifiy_process():
    
    context_conection = conexao()
    if context_conection is None:
          return
    cursor = context_conection.cursor()
