from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_USER = os.getenv('BD_MONGO_USER')
MONGO_PASS = os.getenv('BD_MONGO_PASS')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')  
MONGO_PORT = os.getenv('MONGO_PORT', '27017')      

MONGO_URI_AUTH = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/"

print(f"minha conexao::{MONGO_URI_AUTH}")

try:
        
      Client = MongoClient(MONGO_URI_AUTH)
      Client.admin.command('ping')
      print(f"minha conexao, foi realizada com sucesso de forma local")

except Exception as e:
      print("\n[ERRO] Falha na Conexão ou Autenticação.")
      print(f"Detalhes: {e}")



# Client = MongoClient(f"mongodb+srv://{os.getenv("BD_MONGO_USER")}:{os.getenv("BD_MONGO_PASS")}@cluster0.pdkxk52.mongodb.net/")
# db_con = Client['progesto_Colletion']

# bd = Client.get_database('progesto_Colletion')
# print(f"meu data base{bd}")
# colecao = bd.get_collection('progestor_transaction')


# dado = {
#     "nome": "anderson Oliveira araujo",
#     "telefone":"19971101711"
# }

# colecao.insert_one(dado)

