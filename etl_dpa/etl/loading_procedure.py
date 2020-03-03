'''
TODO: Implementar la carga de un JSON a la tabla de inspections.

El chiste es que esta clase reciba en su constructor la ruta relativa del archivo temporal de json que se acaba de generar para poder leerlo, parsearlo y cargarlo.

En este repoo se tiene de ejemplo el archivo `tmp/inspections_2020_01_31.json`, el cual corresponde al resultado del GET al API con parámetro de fecha del 31 de enero de 2020. 
'''

from etl_dpa.commons.configuration import get_database_connection_url

class LoadingProcedure():
    def __init__(self, json_path):
        self.tmp_json_path = json_path
        #constructor
        #inicializar conexión con postgres
        

    def execute(self):
        return True
        #escribe aquí la lógica para leer el json y cargarlo a postgres. Hay que hacerlo de tal forma que esta clase no cambie cuando queramos apuntar a producción