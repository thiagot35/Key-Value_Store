import socket
import pickle #biblioteca para enviar o objeto Mensagem
import time
import threading
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

#Classe de replicação da Mensagem
def envia_replication(outros_servidores, jsonRepl):


    for porta in outros_servidores:

        #conexão temporária para fazer a replicação da mensagem recebida
        clientRepl1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientRepl1.connect(("127.0.0.1", porta))
        
        #Enviando dados de replicação
        clientRepl1.send(jsonRepl)
        
        #Esperando retorno REPLICATION_OK
        responseRepl1 = clientRepl1.recv(1024)
        recvRepl_Ok = pickle.loads(responseRepl1)
        
        
        
        clientRepl1.close()
                


#Classe que rodará em cada thread processando as requisições recebidas, vindas do cliente ou outro servidor
def thread_req(conn, numporta, ipLider, obj, outros_servidores):
    
    #recebe a requisição
    req = conn.recv(1024)
    #utiliza a biblioteca pickle para abrir o objeto Mensagem
    infoMensagem = pickle.loads(req)
    
    #Atribui as variáveis à serem utilizadas neste servidor tratando a requisição
    key = infoMensagem.key
    value = infoMensagem.value
    timestamp = infoMensagem.timestamp
    modo = infoMensagem.modo
    ipRetorno = infoMensagem.porta_cliente
    
    #print("printando key, porta cliente e modo")
    #print(key)
    #print(ipRetorno)
    #print(modo)
    
    
    #Modo PUT
    if modo == "1":
        
        #Caso o servidor que recebeu a requisição for o servidor líder, ele já a trata diretamente
        if numporta == ipLider:
            
            print(f"Cliente [{ipRetorno}] PUT key:[{key}] value:[{value}]")
            
            if int(obj.get(key)[1]) == 0:
                timestamp = 1
            else:
                timestamp = int(obj.get(key)[1]) + 1            
            
            #insere os dados na tabela Local
            obj.put(key, value, timestamp)

            #Criando mensagem de replicação
            mensagemRepl = Mensagem(key, value, 0, timestamp, "3")
            jsonRepl = pickle.dumps(mensagemRepl)
            
            #Chama classe que faz o envio da replicação e recebe o replication_ok
            envia_replication(outros_servidores, jsonRepl)
            
            #Cria conexão temporária para enviar o PUT_OK do líder diretamente para o cliente
            respostaClientPut = "PUT_OK"
            clientRetorno = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            clientRetorno.connect(("127.0.0.1", ipRetorno))
            
            #Cria o PUT_OK e informações para retorno
            mensagemPutOk = Mensagem(key, value, 0, timestamp, "PUT_OK")
            sndPut_ok = pickle.dumps(mensagemPutOk)
            
            print(f"Enviando PUT_OK ao Cliente [{ipRetorno}] da key:[{key}] ts:[{timestamp}]")
            clientRetorno.send(sndPut_ok)        
            
            
            
            clientRetorno.close()        
            
            
                

        #Caso onde o servidor não é o líder e envia a mensagem para o líder tratar e replicar        
        else:
            
            print(f"Encaminhando PUT key:[{key}] value:[{value}]")
        
            
            #Cria conexão temporária com o líder para repassar a mensagem que será replicada, server não líder
            serverReplication = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            serverReplication.connect(("127.0.0.1", ipLider))
            
            #print("Enviando dados de replicação")
            msgRepl = pickle.dumps(infoMensagem)
            
            serverReplication.send(msgRepl)
            #Nesse caso não espera retorno, apenas repassa a mensagem ao líder
            serverReplication.close()
            #print("Dados enviados para replicação")
            
            
    #Modo GET
    elif modo == "2":
        retornoValueGet = obj.get(key)[0]
        timestamp_servidor = obj.get(key)[1]
        retornoIpGet = numporta
        #print("Recbido do cliente:")
        
        
        #Caso o valor buscado pelo cliente exista no servidor entra na condição abaixo
        if retornoValueGet != "Chave nao encontrada":
            #O Cliente só faz a atualização da tabela local se o timestamp do servidor for maior do que seu timestamp
            if timestamp <= timestamp_servidor:
                retornoTSGet = int(obj.get(key)[1])
            else:
                #Caso o timestamp do servidor for menor do que o do cliente gera o ts de erro abaixo
                retornoTSGet = "TRY_OTHER_SERVER_OR_LATER."
        else:
            retornoTSGet = 0
        
                
        print(f"Cliente [{ipRetorno}] GET key:[{key}] ts:[{timestamp}]. Meu ts é [{timestamp_servidor}], portanto devolvendo [{retornoTSGet}]")
        
        #Mensagem GET
        mensagemGet = Mensagem(key, retornoValueGet, retornoIpGet, retornoTSGet, modo)
        jsonMensagem = pickle.dumps(mensagemGet)

        #envia get para o cliente
        conn.send(jsonMensagem)
        
    #Modo REPLICATION
    elif modo == "3":
                
        #Condição para forçar o TRY_OTHER_SERVER_OR_LATER no Cliente
        #Vamos forçar a replicação errada de uma chave específica
        #Ela ficará atualizada no líder mas não nos outros servidores
        if key == "testeErro" and timestamp >= 3:
            timestamp = 1
        
        
        print(f"REPLICATION key:[{key}] value:[{value}]ts:[{timestamp}]")
        obj.put(key, value, timestamp)
        
        sndRepl_ok = pickle.dumps("REPLICATION_OK")
        
        #Envia o replication ok para o líder
        conn.send(sndRepl_ok)
    
    print("Tabela do Servidor Atualizada:")
    print(obj.__dict__)    
    conn.close()





def main():
    
    #chama o timemap que armazenará as chaves, valores e timestamps de nossa rede
    obj = TimeMap()
    numporta = int(input("Insira a porta deste servidor: "))
    ipLider = int(input("Insira a porta do servidor líder: "))
    
    #servidores padronizados no exercício
    lista_servidores = [10097, 10098, 10099]
    #lista com os outros dois servidores além deste em execução
    outros_servidores = [porta for porta in lista_servidores if porta != numporta]

    print("Inicializando Servidor")
    
    #inicializa o servidor na porta indicada pelo cliente
    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverSocket.bind(("127.0.0.1", numporta))
    serverSocket.listen()

    while True:
        
        print("Aguardando Conexão")
        #Mantém o servidor sempre numa thread para receber requisições
        c, addr = serverSocket.accept()
        threading.Thread(target=thread_req,args=(c, numporta, ipLider, obj, outros_servidores)).start()
        


if __name__ == "__main__":
    main()
