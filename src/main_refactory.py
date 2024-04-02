from refactory import ExtractPipeline, TransformPipeline, LoadPipeline, ConsolidatePipeline
from time import sleep
import os


host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
extract_pipeline = ExtractPipeline(host_postgres)
transform_pipeline = TransformPipeline()
load_pipeline = LoadPipeline()
consolidator = ConsolidatePipeline()



extractor_file_path = r"C:\Users\marcos.silvaext\Documents\01 - INPUT_DATA"
data_raw_path = r"C:\Users\marcos.silvaext\Documents\02 - DATA_RAW"
file_path_error = r"C:\Users\marcos.silvaext\Documents\05 - EXTRATORES COM ERROS"
report_path = r"C:\Users\marcos.silvaext\Documents\04 - REPORTS"
duplicate_file_path = r"C:\Users\marcos.silvaext\Documents\06 - ARQUIVOS DUPLICADOS"

column_name = 'Pedido Faturamento'


while True:
        print('Iniciando extração de novos pedidos...')
        files_data = extract_pipeline.list_files_in_directory(extractor_file_path, file_path_error)

        if files_data:
                extract_pipeline.verify_column(extractor_file_path, files_data, column_name, file_path_error)
                new_orders = extract_pipeline.identify_new_orders(files_data, column_name)

        if new_orders:
                extract_pipeline.create_files_with_new_orders(new_orders, data_raw_path, column_name)
                extract_pipeline.standard_columns_name(new_orders)
                extract_pipeline.add_new_columns_to_database(new_orders)    
                extract_pipeline.update_database(new_orders)
                
        print('Etapa de extração finalizada com sucesso!')   


        raw_files = extract_pipeline.list_files_in_raw_directory(data_raw_path)

        if raw_files:
                
                transform_pipeline.format_columns_values(raw_files)
                transform_pipeline.generate_synthesis_sheet(raw_files, data_raw_path)
                transform_pipeline.format_styles_report_sheet(raw_files, data_raw_path)
                transform_pipeline.format_styles_synthesis_sheet(raw_files, data_raw_path)


        # print('Etapa de formatação finalizada com sucesso!')

        transformated_files = load_pipeline.move_files_to_month_subfolder(data_raw_path, report_path)
        
        """ files_consolidated = consolidator.list_files_to_consolidate(report_path)
        
        if files_consolidated: """
        consolidator.merge_excel_reports(report_path)
        # transform_pipeline.format_styles_report_sheet(files_consolidated, report_path)
        print('Etapa de consolidação finalizada com sucesso!')