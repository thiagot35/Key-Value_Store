import socket
import time
import random
import pickle #biblioteca para enviar o objeto Mensagem
from Mensagem import Mensagem #Classe mensagem que transportará as informações

#A TimeMap será nossa tabela contendo a chave, valor e timestamp
class TimeMap:
    def __init__(self):
        self.keyTimeMap = {}

    def put(self, key, value, now):
        valTimeSt = [value, now]
        self.keyTimeMap[key] = valTimeSt

    def get(self, key):
        if key not in self.keyTimeMap:
            valTimeSt = ["Chave nao encontrada", 0]
            print("Valor nao achado:", valTimeSt[0])
            return valTimeSt
        else:
            return self.keyTimeMap[key]

def main():
    obj = TimeMap()

    while True:
        print("Client iniciado")
        random_num = random.randint(10097, 10099)
        print("Servidor sorteado:", random_num)
        
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", random_num))

        print("Insira qual modo deseja:\n[1]PUT\n[2]GET\n")
        modo = input()
        
        
        #Modo PUT
        if modo == "1":
            print("Insira a key:")
            key = input()

            print("Insira o value:")
            value = input()

            #now = int(time.time() * 1000)
            
            random_ip_cliente = random.randint(1000, 10000)
            #chave, valor, ip_retorno, timestamp, modo
            msg = Mensagem(key, value, random_ip_cliente, -1, modo)
            #Mandamos o timestamp como -1 pois ele não será considerado no put do servidor
            #o timestamp do >>PUT<< é definido apenas no servidor líder pois não terá concorrência
            

            pickled_msg = pickle.dumps(msg)
            

            client.send(pickled_msg)
            
            #Cria um socket para receber put_ok do servidor
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind(("127.0.0.1", random_ip_cliente))
            server_socket.listen()
            
            #esperando put_ok do servidor com as informações
            servidor_client, _ = server_socket.accept()

          
            #key, value, timestamp à serem adicionados à tabela local
            respPutOkTS = servidor_client.recv(1024)
            msgPutOk = pickle.loads(respPutOkTS)
            
            keyPutOk = msgPutOk.key
            valuePutOk = msgPutOk.value
            timestampPutOk = msgPutOk.timestamp
            putOk = msgPutOk.modo
                        
            
            obj.put(keyPutOk, valuePutOk, timestampPutOk)
            print(f"{putOk}; key: [{keyPutOk}] value: [{valuePutOk}]; timestamp: [{timestampPutOk}]")
            
            #fecha a conexão
            server_socket.close()

        
        #Modo GET
        elif modo == "2":
            print("Insira a key desejada:")
            key_get = input()
            
            #timestamp da tabela do cliente e Mensagem GET
            time_stmp = int(obj.get(key_get)[1]) if obj.get(key_get)[1] != 0 else 0
            msg_get = Mensagem(key_get, "", 0, time_stmp, modo)
            
            #Envia Requisição GET
            pickled_msg_get = pickle.dumps(msg_get)           
            client.send(pickled_msg_get)
            
            #Esperando retorno do GET
            responseGet = client.recv(1024)
            retornoServidor = pickle.loads(responseGet)

            #Valores retornados pelo GET            
            value_get = retornoServidor.value
            timestamp_get = retornoServidor.timestamp
            ip_get = retornoServidor.porta_cliente
            
            #Caso o valor buscado pelo cliente exista no servidor entra na condição abaixo
            if value_get != "Chave nao encontrada":
                #O Cliente só faz a atualização da tabela local se o timestamp do servidor for maior do que seu timestamp
                if timestamp_get != "TRY_OTHER_SERVER_OR_LATER.":
                
                    obj.put(key_get, value_get, timestamp_get)
                    print(f"GET key: [{key_get}] value: [{value_get}] obtido do servidor [{ip_get}], meu timestamp [{time_stmp}] e do servidor [{timestamp_get}]")
                    
                else:
                    #Caso o timestamp do servidor for menor do que o do cliente exibe a mensagem abaixo para o cliente
                    print("TRY_OTHER_SERVER_OR_LATER.")


        print("Tabela do Cliente Atualizada:")
        print(obj.__dict__)
        client.close()

if __name__ == "__main__":
    main()
