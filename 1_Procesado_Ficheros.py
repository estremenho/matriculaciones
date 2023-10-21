import os
import pandas as pd
import pyarrow as pa
import sweetviz as sv
import zipfile
import datetime

def convert_file_to_utf8(input_file, output_file):
    # Load file
    with open(input_file, 'r', encoding='ISO-8859-1') as f_in:
        input_file_read = f_in.read()
    
    os.remove(input_file)
    
    # Transcode
    print('---- Transcoding…')
    with open(output_file, 'x', encoding='utf-8', newline='\n') as f_out:
        f_out.write(input_file_read)
        
    print('---- Transcoding Done')

def clean_dates(x):
    return datetime.strptime(x, '%d%m%Y') if x != '?' else None 

def clean_numbers(x):
    return float(x) if x != '*******' else None 
    
def process_data_dgt(filespath, file_zip):
    
    widths = [8,1,8,30,22,1,21,2,1,5,6,6,6,3,2,2,2,2,24,2,2,1,8,5,8,1,1,9,3,5,
              30,7,3,5,1,1,1,1,1,1,11,25,25,35,70,6,6,4,4,3,8,4,4,4,6,30,50,35,
              25,35,4,4,4,1,25,1,4,25,8]
    
    types_dict = {'FEC_MATRICULA': str,
        'COD_CLASE_MAT': int,
        'FEC_TRAMITACION': str,
        'MARCA_ITV': str,
        'MODELO_ITV': str,
        'COD_PROCEDENCIA_ITV': str,
        'BASTIDOR_ITV': str,
        'COD_TIPO': str,
        'COD_PROPULSION_ITV': str,
        'CILINDRADA_ITV': float,
        'POTENCIA_ITV': float,
        'TARA': float,
        'PESO_MAX': float,
        'NUM_PLAZAS': int,
        'IND_PRECINTO': str,
        'IND_EMBARGO': str,
        'NUM_TRANSMISIONES': int,
        'NUM_TITULARES': int,
        'LOCALIDAD_VEHICULO': str,
        'COD_PROVINCIA_VEH': str,
        'COD_PROVINCIA_MAT': str,
        'CLAVE_TRAMITE': str,
        'FEC_TRAMITE': str,
        'CODIGO_POSTAL': str,
        'FEC_PRIM_MATRICULACION': str,
        'IND_NUEVO_USADO': str,
        'PERSONA_FISICA_JURIDICA': str,
        'CODIGO_ITV': str,
        'SERVICIO': str,
        'COD_MUNICIPIO_INE_VEH': str,
        'MUNICIPIO': str,
        'KW_ITV': str,
        'NUM_PLAZAS_MAX': int,
        'CO2_ITV': float,
        'RENTING': str,
        'COD_TUTELA': str,
        'COD_POSESION': str,
        'IND_BAJA_DEF': str,
        'IND_BAJA_TEMP': str,
        'IND_SUSTRACCION': str,
        'BAJA_TELEMATICA': str,
        'TIPO_ITV': str,
        'VARIANTE_ITV': str,
        'VERSION_ITV': str,
        'FABRICANTE_ITV': str,
        'MASA_ORDEN_MARCHA_ITV': str,
        'MASA_MÁXIMA_TECNICA_ADMISIBLE_ITV': str,
        'CATEGORÍA_HOMOLOGACIÓN_EUROPEA_ITV': str,
        'CARROCERIA': str,
        'PLAZAS_PIE': str,
        'NIVEL_EMISIONES_EURO_ITV': str,
        'CONSUMO_WH/KM_ITV': str,
        'CLASIFICACIÓN_REGLAMENTO_VEHICULOS_ITV': str,
        'CATEGORÍA_VEHÍCULO_ELÉCTRICO': str,
        'AUTONOMÍA_VEHÍCULO_ELÉCTRICO': str,
        'MARCA_VEHÍCULO_BASE': str,
        'FABRICANTE_VEHÍCULO_BASE': str,
        'TIPO_VEHÍCULO_BASE': str,
        'VARIANTE_VEHÍCULO_BASE': str,
        'VERSIÓN_VEHÍCULO_BASE': str,
        'DISTANCIA_EJES_12': str,
        'VIA_ANTERIOR_ITV': str,
        'VIA_POSTERIOR_ITV': str,
        'TIPO_ALIMENTACION_ITV': str,
        'CONTRASEÑA_HOMOLOGACION_ITV': str,
        'ECO_INNOVACION_ITV': str,
        'REDUCCION_ECO_ITV': str,
        'CODIGO_ECO_ITV': str,
        'FEC_PROCESO': str,
        } 

    print('-- Decompress file…')

    with zipfile.ZipFile(filespath+file_zip,"r") as zip_ref:
        zip_ref.extractall(path=filespath, members=None, pwd=None)
        
    file_txt = zip_ref.namelist()[0]
    
    print('-- Change encode to UTF-8')
    convert_file_to_utf8(filespath+file_txt, filespath+file_txt)
        
    print('-- Transform to dataframe…')
    df = pd.read_fwf(filespath+file_txt, widths=widths, header=None, skiprows=1
                    , names = list(types_dict.keys()), dtype= types_dict)
    
    (
        df.assign(
            FEC_MATRICULA = lambda df: df['FEC_MATRICULA'].map(clean_dates),
            FEC_TRAMITACION = lambda df: df['FEC_TRAMITACION'].map(clean_dates),
            FEC_TRAMITE = lambda df: df['FEC_TRAMITE'].map(clean_dates),
            FEC_PRIM_MATRICULACION = lambda df: df['FEC_PRIM_MATRICULACION'].map(clean_dates),
            FEC_PROCESO = lambda df: df['FEC_PROCESO'].map(clean_dates),
            KM_IVT = lambda df: df['KM_IVT'].map(clean_numbers)
        )
    )
    
    df.replace('^\s+', '', regex=True, inplace=True) #front
    df.replace('\s+$', '', regex=True, inplace=True) #end
    
    os.remove(filespath+file_txt)
    
    return df
    
df = process_data_dgt("", "export_mensual_mat_202304.zip")

dq_report = sv.analyze(df)
dq_report.show_html('Advertising.html')





    print('-- Creating arrow file')
    arrow_file = file_txt.replace(".txt", ".arrow")
    table = pa.Table.from_pandas(df)
    with pa.OSFile(filespath+arrow_file, 'wb') as sink:
        with pa.RecordBatchFileWriter(sink, table.schema) as writer:
            writer.write_table(table)