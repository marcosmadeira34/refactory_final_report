from refactory import ExtractPipeline, TransformPipeline, LoadPipeline, ConsolidatePipeline
from time import sleep
import os

# DIRETÓRIOS PARA SERVIDOR LINUX

# DIRETÓRIO DE ENTRADA DOS ARQUIVOS EXTRATORES (EXTRACTION)
extractor_file_path = r"/home/administrator/WindowsShare/01 - FATURAMENTO/00-EXTRATOR_PEDIDOS_DE_CLIENTES" # EXTRATOR

# # DIRETÓRIOS DE SAÍDA DOS ARQUIVOS CRIADOS (LOADING)
batch_totvs_path = r'/home/administrator/WindowsShare/01 - FATURAMENTO/01 - CLIENTES - CONTROLE - 2024 TOTVS' # CRIARÁ AS PASTA AQUI

# DIRETÓRIO DE TRATAMENTO DOS ARQUIVOS (TRANSFORMATION)
data_raw_path = r'/home/administrator/WindowsShare/01 - FATURAMENTO/02 - DATA_RAW' # NOVOS PEDIDOS IDENTIFICADOS NO EXTRATOR
source_directory = r'/home/administrator/WindowsShare/01 - FATURAMENTO/02 - DATA_RAW' # DIRETÓRIO DE ORIGEM DOS PEDIDOS
report_path = r'/home/administrator/WindowsShare/01 - FATURAMENTO/01 - CLIENTES - CONTROLE - 2024 TOTVS' # DIRETÓRIO DE DESTINO DOS PEDIDOS

# # DIRETÓRIO DE ARQUIVOS PROCESSADOS (DRAFT)
process_files = r'/home/administrator/WindowsShare/01 - FATURAMENTO/04 - EXTRATORES PROCESSADOS'

file_path_error = r'/home/administrator/WindowsShare/01 - FATURAMENTO/05 - EXTRATORES COM ERROS' # ARQUIVOS COM ERROS NO EXTRATOR

# DIRETÓRIOS AUXILIARES (SANDBOX)
output_merge_path = r'C:/DataWare/data/consolidated_files/consolidated_validated/MERGE_RELATÓRIO_FINAL' # RELATÓRIO FINAL 
invoiced_orders = r'C:/DataWare/data/consolidated_files/consolidated_validated/PEDIDOS_FATURADOS' # PEDIDOS FATURADOS NO BANCO DE DADOS

# INSTÂNCIANDO AS CLASSES

host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
extract_pipeline = ExtractPipeline(host_postgres)
transform_pipeline = TransformPipeline()
load_pipeline = LoadPipeline()
consolidator = ConsolidatePipeline()

# DIRETÓRIOS PARA SERVIDOR WINDOWS
""" 
extractor_file_path = r"C:\Users\marcos.silvaext\Documents\01 - INPUT_DATA"
data_raw_path = r"C:\Users\marcos.silvaext\Documents\02 - DATA_RAW"
file_path_error = r"C:\Users\marcos.silvaext\Documents\05 - EXTRATORES COM ERROS"
report_path = r"C:\Users\marcos.silvaext\Documents\04 - REPORTS"
duplicate_file_path = r"C:\Users\marcos.silvaext\Documents\06 - ARQUIVOS DUPLICADOS" """

column_name = 'Pedido Faturamento'

new_orders = None 

while True:
        print('Iniciando extração de novos pedidos...')
        files_data = extract_pipeline.list_files_in_directory(extractor_file_path, file_path_error)
        
        
        extract_pipeline.verify_column(extractor_file_path, files_data, column_name, file_path_error)
        new_orders = extract_pipeline.identify_new_orders(files_data, column_name)
                

        
        extract_pipeline.create_files_with_new_orders(new_orders, data_raw_path, column_name)
        extract_pipeline.standard_columns_name(new_orders)
        extract_pipeline.add_new_columns_to_database(new_orders)    
        extract_pipeline.update_database(new_orders)
             
        
        raw_files = extract_pipeline.list_files_in_raw_directory(data_raw_path)

        
                
        transform_pipeline.format_columns_values(raw_files)
                # transform_pipeline.format_columns_cnpj(raw_files)
                # transform_pipeline.format_columns_date(raw_files)
        transform_pipeline.generate_synthesis_sheet(raw_files, data_raw_path)
                # transform_pipeline.format_billing_values(raw_files)
        transform_pipeline.format_styles_report_sheet(raw_files, data_raw_path)
        transform_pipeline.format_styles_synthesis_sheet(raw_files, data_raw_path)


        # print('Etapa de formatação finalizada com sucesso!')

        transformated_files = load_pipeline.move_files_to_month_subfolder(data_raw_path, report_path)
       
       
        consolidator.merge_excel_reports(report_path)
        # transform_pipeline.format_styles_report_sheet(files_consolidated, report_path)
        print('Etapa de consolidação finalizada com sucesso!')







        