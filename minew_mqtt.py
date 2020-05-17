
#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio
import mqttools
import logging
import json

import pyodbc


####################################################
#                      Classes                     #
####################################################

class Gateway():

    def __init__(self, message, name):
        self.type= message["type"]
        self.mac= message["mac"]
        self.timestamp= message["timestamp"]
        self.name=name

    def __str__(self):
        return str(self.mac)
        
    def __eq__(self, other):
        return self.__str__() == other.__str__()

    def get(self, attribute):
        if attribute == "type":
            return self.type
        elif attribute in "mac":
            return self.mac
        elif attribute == "timestamp":
            return self.timestamp
        elif attribute == "name":
            return self.name

        else:
            return None

 
    async def write_edificio(self, conn):
        cursor = conn.cursor()
        cursor.execute(
            'BEGIN TRY '
                'INSERT INTO EDIFICIO'
                        '(MAC'
                        ',NAME'
                        ',OP_TIMESTAMP) '
                    'VALUES '
                        '(?,'
                        '?, '
                        '?) '
                'END TRY '
                'BEGIN CATCH '
                    'IF ERROR_NUMBER() = 2627 '
                        'update EDIFICIO set NAME= ?, OP_TIMESTAMP= ? where MAC = ? ; '
                'END CATCH',
            (self.mac , self.name, self.timestamp, self.name, self.timestamp, self.mac)
        )
        conn.commit()
        #read(conn)

class Beacon():

    def __init__(self, message, name):
        self.type= message["type"]
        self.mac= message["mac"]
        self.timestamp= message["timestamp"]
        self.ibeaconUuid= message["ibeaconUuid"]
        self.ibeaconMajor= message["ibeaconMajor"]
        self.ibeaconMinor= message["ibeaconMinor"]
        self.rssi= message["rssi"]
        self.ibeaconTxPower= message["ibeaconTxPower"]
        self.name=name


    def __str__(self):
        return str(self.mac)

    def __eq__(self, other):
        return self.__str__() == other.__str__()

    def get(self, attribute):
        if attribute == "type":
            return self.type
        elif attribute in "mac":
            return self.mac
        elif attribute == "timestamp":
            return self.timestamp
        elif attribute in "ibeaconUuid":
            return self.ibeaconUuid
        elif attribute == "ibeaconMajor":
            return self.ibeaconMajor
        elif attribute == "ibeaconMinor":
            return self.ibeaconMinor
        elif attribute == "rssi":
            return self.rssi
        elif attribute == "ibeaconTxPower":
            return self.ibeaconTxPower
        elif attribute == "name":
            return self.name

        else:
            return None

 
    async def write_empregado(self, conn):
        cursor = conn.cursor()
        cursor.execute(
            'BEGIN TRY '
                'INSERT INTO EMPREGADO'
                        '(MAC'
                        ',RSSI'
                        ',NAME'
                        ',OP_TIMESTAMP) '
                    'VALUES'
                        '(?,'
                        '?, '
                        '?, '
                        '?) '
                'END TRY '
                'BEGIN CATCH '
                    'IF ERROR_NUMBER() = 2627 '
                        'update EMPREGADO set RSSI= ?, NAME= ?, OP_TIMESTAMP= ? where MAC = ? ; '
                'END CATCH',
            (self.mac , self.rssi, self.name, self.timestamp, self.rssi, self.name, self.timestamp, self.mac)
        )
        conn.commit()
        #read(conn)

####################################################
#          Universal Database Functions            #
####################################################
async def read(conn, table="dummy"):
    print("\n\n")
    #print("___________Read_________\n")
    print(f"___________{table}_________\n")

    cursor = conn.cursor()
    
    cursor.execute(
        f"select * from {table}"
    )
    for i, row in enumerate(cursor):
        print(f'row {i+1}: {row}')
    print("\n")

async def create_empregado(conn, table="EMPREGADO"):
    print(f"__________Create__{table}________\n")
    cursor = conn.cursor()
    cursor.execute(
        'CREATE TABLE EMPREGADO ('
            'MAC nchar(12) NOT NULL PRIMARY KEY,'
            'RSSI int,'
            'NAME varchar(100),'	
            'OP_TIMESTAMP DATETIME' 
        ');'
    )
    conn.commit()


async def create_edificio(conn, table="EDIFICIO"):
    print(f"__________Create__{table}________\n")
    cursor = conn.cursor()
    cursor.execute(
        'CREATE TABLE EDIFICIO ('
            'MAC nchar(12) NOT NULL PRIMARY KEY,'
            'NAME varchar(100),'	
            'OP_TIMESTAMP DATETIME '
        ');'
    )
    conn.commit()



async def insert(conn):
    print("__________insert________\n")
    cursor = conn.cursor()
    cursor.execute(
        'insert into dummy(a,b) values(?,?);',
        (3232, 'catzzz')
    )
    conn.commit()
    read(conn)

async def update_gateway(conn, table= 'dummy'):
    print(f"_________Update__{table}_______\n")
    cursor = conn.cursor()
    cursor.execute(
        'update EMPREGADO set NAME=? where MAC = ?;',
        ('dogzzz', 'AC233FA36C2C')
    )
    conn.commit()
    read(conn) 

async def update_empregado(conn, table= 'dummy'):
    print(f"_________Update__{table}_______\n")
    cursor = conn.cursor()
    cursor.execute(
        'update EMPREGADO set RSSI= ?, NAME=? where MAC = ?;',
        (-1 ,'dogzzz', 'AC233FA36C2C')
    )
    conn.commit()
    read(conn) 

async def delete(conn, table= 'dummy'):
    print(f"_________Delete__{table}_______\n")
    cursor = conn.cursor()
    cursor.execute(
        f'delete TOP (100) from {table};'
    )
    conn.commit()
    await read(conn)

###################################################

async def db_sender(conn, message):

    edificio_dic={"AC233FC067F5" : "Pintura"}

    colaborador_dic={"AC233FA36C2C": "Xavier", "AC233F5E6896": "Fábio"}


    for m in message:
        if m["type"]== "Gateway":

            gateway=Gateway(m, edificio_dic[str(m["mac"])])
            await gateway.write_edificio(conn)
            print("debug ==> Gateway of: ", edificio_dic[str(gateway)], ' | Timestamp: ', gateway.get("timestamp"))

            
        elif m["type"]== "iBeacon":

            beacon=Beacon(m, colaborador_dic[str(m["mac"])])
            await beacon.write_empregado(conn)
            print("debug ==> iBeacon of: ", colaborador_dic[str(beacon)]," | RSSI: ", beacon.get("rssi"))

            
        else:
            pass
        
    await read(conn, "EDIFICIO")
    await read(conn, "EMPREGADO")



async def handle_messages(client, conn,  debug= False):
    while True:
        topic, message = await client.messages.get()
        

        if topic is None:
            print('Connection lost.')
            break
        message=json.loads(str(message.decode("utf-8","ignore")))
        if debug:
            print("-------------------------------------------------------------------------")
            print(f'Got {message} on {topic}.')
            print("-------------------------------------------------------------------------")
            print("\n")
            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

            for i, m in enumerate(message):
                print("--------------------------------------")
                print("# MESSAGE OF: ", m["type"],  " Nº: ", i, "#")
                print("--------------------------------------")
                for key, value in m.items() :
                    print (key,": ", value)
                
                print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            await db_sender(conn, message)



async def reconnector():
    client = mqttools.Client('racelandsa.dyndns.org',
                             1883,
                             subscriptions=['/gw/ac233fc067f5/status'],
                             connect_delays=[1, 2, 4, 8])
    
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=DESKTOP-D3SGQ51\TEW_SQLEXPRESS;"
        "Database=race_test;"
        "Trusted_Connection=yes;"
    )

    #await delete(conn, 'EMPREGADO')
    #await delete(conn, 'EDIFICIO')
    await create_edificio(conn)
    await create_empregado(conn)

    while True:
        #break
        await client.start()
        await handle_messages(client, conn, debug=True)
        await client.stop()
        await conn.close()

logging.basicConfig(level=logging.INFO)

asyncio.run(reconnector()) 