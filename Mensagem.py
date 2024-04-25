class Mensagem:

    def __init__(self, key, value, porta_cliente, timestamp, modo):
        self.key = key
        self.value = value
        self.porta_cliente = porta_cliente
        self.timestamp = timestamp
        self.modo = modo

    def get_key(self):
        return self.key

    def get_value(self):
        return self.value

    def get_porta_cliente(self):
        return self.porta_cliente

    def get_timestamp(self):
        return self.timestamp

    def get_modo(self):
        return self.modo
